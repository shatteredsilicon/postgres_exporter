package main

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/blang/semver"
	_ "github.com/lib/pq"
	"gopkg.in/ini.v1"
	"gopkg.in/yaml.v2"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"

	"strings"

	"github.com/shatteredsilicon/exporter_shared"
)

var (
	showVersion = flag.Bool(
		"version", false,
		"Print version information.",
	)
	configPath = flag.String(
		"config", "/opt/ss/ssm-client/postgres_exporter.conf",
		"Path of config file",
	)
	listenAddress = flag.String(
		"web.listen-address", getStringEnv("PG_EXPORTER_WEB_LISTEN_ADDRESS", ":9184"),
		"Address to listen on for web interface and telemetry.",
	)
	metricsPath = flag.String(
		"web.telemetry-path", getStringEnv("PG_EXPORTER_WEB_TELEMETRY_PATH", "/metrics"),
		"Path under which to expose metrics.",
	)
	disableDefaultMetrics = flag.Bool(
		"disable-default-metrics", getBoolEnv("PG_EXPORTER_DISABLE_DEFAULT_METRICS", false),
		"Do not include default metrics.",
	)
	queriesPath = flag.String(
		"extend.query-path", getStringEnv("PG_EXPORTER_EXTEND_QUERY_PATH", ""),
		"Path to custom queries to run.",
	)
	onlyDumpMaps = flag.Bool(
		"dumpmaps", false,
		"Do not run, simply dump the maps.",
	)
)

// Metric name parts.
const (
	// Namespace for all metrics.
	namespace = "pg"
	// Subsystems.
	exporter = "exporter"
	// Metric label used for static string data thats handy to send to Prometheus
	// e.g. version
	staticLabelName = "static"
)

func init() {
	prometheus.MustRegister(version.NewCollector("postgres_exporter"))
}

// ColumnUsage should be one of several enum values which describe how a
// queried row is to be converted to a Prometheus metric.
type ColumnUsage int

// nolint: golint
const (
	DISCARD      ColumnUsage = iota // Ignore this column
	LABEL        ColumnUsage = iota // Use this column as a label
	COUNTER      ColumnUsage = iota // Use this column as a counter
	GAUGE        ColumnUsage = iota // Use this column as a gauge
	MAPPEDMETRIC ColumnUsage = iota // Use this column with the supplied mapping of text values
	DURATION     ColumnUsage = iota // This column should be interpreted as a text duration (and converted to milliseconds)
)

// UnmarshalYAML implements the yaml.Unmarshaller interface.
func (cu *ColumnUsage) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var value string
	if err := unmarshal(&value); err != nil {
		return err
	}

	columnUsage, err := stringToColumnUsage(value)
	if err != nil {
		return err
	}

	*cu = columnUsage
	return nil
}

// Regex used to get the "short-version" from the postgres version field.
var versionRegex = regexp.MustCompile(`^\w+ ((\d+)(\.\d+)?(\.\d+)?)`)
var lowestSupportedVersion = semver.MustParse("9.1.0")

// Parses the version of postgres into the short version string we can use to
// match behaviors.
func parseVersion(versionString string) (semver.Version, error) {
	submatches := versionRegex.FindStringSubmatch(versionString)
	if len(submatches) > 1 {
		return semver.ParseTolerant(submatches[1])
	}
	return semver.Version{},
		errors.New(fmt.Sprintln("Could not find a postgres version in string:", versionString))
}

// ColumnMapping is the user-friendly representation of a prometheus descriptor map
type ColumnMapping struct {
	usage             ColumnUsage        `yaml:"usage"`
	description       string             `yaml:"description"`
	mapping           map[string]float64 `yaml:"metric_mapping"` // Optional column mapping for MAPPEDMETRIC
	supportedVersions semver.Range       `yaml:"pg_version"`     // Semantic version ranges which are supported. Unsupported columns are not queried (internally converted to DISCARD).
}

// UnmarshalYAML implements yaml.Unmarshaller
func (cm *ColumnMapping) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain ColumnMapping
	return unmarshal((*plain)(cm))
}

// MetricMapNamespace groups metric maps under a shared set of labels.
type MetricMapNamespace struct {
	labels         []string             // Label names for this namespace
	columnMappings map[string]MetricMap // Column mappings in this namespace
}

// MetricMap stores the prometheus metric description which a given column will
// be mapped to by the collector
type MetricMap struct {
	discard    bool                              // Should metric be discarded during mapping?
	vtype      prometheus.ValueType              // Prometheus valuetype
	desc       *prometheus.Desc                  // Prometheus descriptor
	conversion func(interface{}) (float64, bool) // Conversion function to turn PG result into float64
}

// TODO: revisit this with the semver system
func dumpMaps() {
	// TODO: make this function part of the exporter
	for name, cmap := range builtinMetricMaps {
		query, ok := queryOverrides[name]
		if !ok {
			fmt.Println(name)
		} else {
			for _, queryOverride := range query {
				fmt.Println(name, queryOverride.versionRange, queryOverride.query)
			}
		}

		for column, details := range cmap {
			fmt.Printf("  %-40s %v\n", column, details)
		}
		fmt.Println()
	}
}

var builtinMetricMaps = map[string]map[string]ColumnMapping{
	"pg_stat_bgwriter": {
		"checkpoints_timed":     {COUNTER, "Number of scheduled checkpoints that have been performed", nil, nil},
		"checkpoints_req":       {COUNTER, "Number of requested checkpoints that have been performed", nil, nil},
		"checkpoint_write_time": {COUNTER, "Total amount of time that has been spent in the portion of checkpoint processing where files are written to disk, in milliseconds", nil, nil},
		"checkpoint_sync_time":  {COUNTER, "Total amount of time that has been spent in the portion of checkpoint processing where files are synchronized to disk, in milliseconds", nil, nil},
		"buffers_checkpoint":    {COUNTER, "Number of buffers written during checkpoints", nil, nil},
		"buffers_clean":         {COUNTER, "Number of buffers written by the background writer", nil, nil},
		"maxwritten_clean":      {COUNTER, "Number of times the background writer stopped a cleaning scan because it had written too many buffers", nil, nil},
		"buffers_backend":       {COUNTER, "Number of buffers written directly by a backend", nil, nil},
		"buffers_backend_fsync": {COUNTER, "Number of times a backend had to execute its own fsync call (normally the background writer handles those even when the backend does its own write)", nil, nil},
		"buffers_alloc":         {COUNTER, "Number of buffers allocated", nil, nil},
		"stats_reset":           {COUNTER, "Time at which these statistics were last reset", nil, nil},
	},
	"pg_stat_database": {
		"datid":          {LABEL, "OID of a database", nil, nil},
		"datname":        {LABEL, "Name of this database", nil, nil},
		"numbackends":    {GAUGE, "Number of backends currently connected to this database. This is the only column in this view that returns a value reflecting current state; all other columns return the accumulated values since the last reset.", nil, nil},
		"xact_commit":    {COUNTER, "Number of transactions in this database that have been committed", nil, nil},
		"xact_rollback":  {COUNTER, "Number of transactions in this database that have been rolled back", nil, nil},
		"blks_read":      {COUNTER, "Number of disk blocks read in this database", nil, nil},
		"blks_hit":       {COUNTER, "Number of times disk blocks were found already in the buffer cache, so that a read was not necessary (this only includes hits in the PostgreSQL buffer cache, not the operating system's file system cache)", nil, nil},
		"tup_returned":   {COUNTER, "Number of rows returned by queries in this database", nil, nil},
		"tup_fetched":    {COUNTER, "Number of rows fetched by queries in this database", nil, nil},
		"tup_inserted":   {COUNTER, "Number of rows inserted by queries in this database", nil, nil},
		"tup_updated":    {COUNTER, "Number of rows updated by queries in this database", nil, nil},
		"tup_deleted":    {COUNTER, "Number of rows deleted by queries in this database", nil, nil},
		"conflicts":      {COUNTER, "Number of queries canceled due to conflicts with recovery in this database. (Conflicts occur only on standby servers; see pg_stat_database_conflicts for details.)", nil, nil},
		"temp_files":     {COUNTER, "Number of temporary files created by queries in this database. All temporary files are counted, regardless of why the temporary file was created (e.g., sorting or hashing), and regardless of the log_temp_files setting.", nil, nil},
		"temp_bytes":     {COUNTER, "Total amount of data written to temporary files by queries in this database. All temporary files are counted, regardless of why the temporary file was created, and regardless of the log_temp_files setting.", nil, nil},
		"deadlocks":      {COUNTER, "Number of deadlocks detected in this database", nil, nil},
		"blk_read_time":  {COUNTER, "Time spent reading data file blocks by backends in this database, in milliseconds", nil, nil},
		"blk_write_time": {COUNTER, "Time spent writing data file blocks by backends in this database, in milliseconds", nil, nil},
		"stats_reset":    {COUNTER, "Time at which these statistics were last reset", nil, nil},
	},
	"pg_stat_database_conflicts": {
		"datid":            {LABEL, "OID of a database", nil, nil},
		"datname":          {LABEL, "Name of this database", nil, nil},
		"confl_tablespace": {COUNTER, "Number of queries in this database that have been canceled due to dropped tablespaces", nil, nil},
		"confl_lock":       {COUNTER, "Number of queries in this database that have been canceled due to lock timeouts", nil, nil},
		"confl_snapshot":   {COUNTER, "Number of queries in this database that have been canceled due to old snapshots", nil, nil},
		"confl_bufferpin":  {COUNTER, "Number of queries in this database that have been canceled due to pinned buffers", nil, nil},
		"confl_deadlock":   {COUNTER, "Number of queries in this database that have been canceled due to deadlocks", nil, nil},
	},
	"pg_locks": {
		"datname": {LABEL, "Name of this database", nil, nil},
		"mode":    {LABEL, "Type of Lock", nil, nil},
		"count":   {GAUGE, "Number of locks", nil, nil},
	},
	"pg_stat_replication": {
		"procpid":                  {DISCARD, "Process ID of a WAL sender process", nil, semver.MustParseRange("<9.2.0")},
		"pid":                      {DISCARD, "Process ID of a WAL sender process", nil, semver.MustParseRange(">=9.2.0")},
		"usesysid":                 {DISCARD, "OID of the user logged into this WAL sender process", nil, nil},
		"usename":                  {DISCARD, "Name of the user logged into this WAL sender process", nil, nil},
		"application_name":         {DISCARD, "Name of the application that is connected to this WAL sender", nil, nil},
		"client_addr":              {LABEL, "IP address of the client connected to this WAL sender. If this field is null, it indicates that the client is connected via a Unix socket on the server machine.", nil, nil},
		"client_hostname":          {DISCARD, "Host name of the connected client, as reported by a reverse DNS lookup of client_addr. This field will only be non-null for IP connections, and only when log_hostname is enabled.", nil, nil},
		"client_port":              {DISCARD, "TCP port number that the client is using for communication with this WAL sender, or -1 if a Unix socket is used", nil, nil},
		"backend_start":            {DISCARD, "with time zone	Time when this process was started, i.e., when the client connected to this WAL sender", nil, nil},
		"backend_xmin":             {DISCARD, "The current backend's xmin horizon.", nil, nil},
		"state":                    {LABEL, "Current WAL sender state", nil, nil},
		"sent_location":            {DISCARD, "Last transaction log position sent on this connection", nil, semver.MustParseRange("<10.0.0")},
		"write_location":           {DISCARD, "Last transaction log position written to disk by this standby server", nil, semver.MustParseRange("<10.0.0")},
		"flush_location":           {DISCARD, "Last transaction log position flushed to disk by this standby server", nil, semver.MustParseRange("<10.0.0")},
		"replay_location":          {DISCARD, "Last transaction log position replayed into the database on this standby server", nil, semver.MustParseRange("<10.0.0")},
		"sent_lsn":                 {DISCARD, "Last transaction log position sent on this connection", nil, semver.MustParseRange(">=10.0.0")},
		"write_lsn":                {DISCARD, "Last transaction log position written to disk by this standby server", nil, semver.MustParseRange(">=10.0.0")},
		"flush_lsn":                {DISCARD, "Last transaction log position flushed to disk by this standby server", nil, semver.MustParseRange(">=10.0.0")},
		"replay_lsn":               {DISCARD, "Last transaction log position replayed into the database on this standby server", nil, semver.MustParseRange(">=10.0.0")},
		"sync_priority":            {DISCARD, "Priority of this standby server for being chosen as the synchronous standby", nil, nil},
		"sync_state":               {DISCARD, "Synchronous state of this standby server", nil, nil},
		"slot_name":                {LABEL, "A unique, cluster-wide identifier for the replication slot", nil, semver.MustParseRange(">=9.2.0")},
		"plugin":                   {DISCARD, "The base name of the shared object containing the output plugin this logical slot is using, or null for physical slots", nil, nil},
		"slot_type":                {DISCARD, "The slot type - physical or logical", nil, nil},
		"datoid":                   {DISCARD, "The OID of the database this slot is associated with, or null. Only logical slots have an associated database", nil, nil},
		"database":                 {DISCARD, "The name of the database this slot is associated with, or null. Only logical slots have an associated database", nil, nil},
		"active":                   {DISCARD, "True if this slot is currently actively being used", nil, nil},
		"active_pid":               {DISCARD, "Process ID of a WAL sender process", nil, nil},
		"xmin":                     {DISCARD, "The oldest transaction that this slot needs the database to retain. VACUUM cannot remove tuples deleted by any later transaction", nil, nil},
		"catalog_xmin":             {DISCARD, "The oldest transaction affecting the system catalogs that this slot needs the database to retain. VACUUM cannot remove catalog tuples deleted by any later transaction", nil, nil},
		"restart_lsn":              {DISCARD, "The address (LSN) of oldest WAL which still might be required by the consumer of this slot and thus won't be automatically removed during checkpoints", nil, nil},
		"pg_current_xlog_location": {DISCARD, "pg_current_xlog_location", nil, nil},
		"pg_current_wal_lsn":       {DISCARD, "pg_current_xlog_location", nil, semver.MustParseRange(">=10.0.0")},
		"pg_xlog_location_diff":    {GAUGE, "Lag in bytes between master and slave", nil, semver.MustParseRange(">=9.2.0 <10.0.0")},
		"pg_wal_lsn_diff":          {GAUGE, "Lag in bytes between master and slave", nil, semver.MustParseRange(">=10.0.0")},
		"confirmed_flush_lsn":      {DISCARD, "LSN position a consumer of a slot has confirmed flushing the data received", nil, nil},
		"write_lag":                {DISCARD, "Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written it (but not yet flushed it or applied it). This can be used to gauge the delay that synchronous_commit level remote_write incurred while committing if this server was configured as a synchronous standby.", nil, semver.MustParseRange(">=10.0.0")},
		"flush_lag":                {DISCARD, "Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written and flushed it (but not yet applied it). This can be used to gauge the delay that synchronous_commit level remote_flush incurred while committing if this server was configured as a synchronous standby.", nil, semver.MustParseRange(">=10.0.0")},
		"replay_lag":               {DISCARD, "Time elapsed between flushing recent WAL locally and receiving notification that this standby server has written, flushed and applied it. This can be used to gauge the delay that synchronous_commit level remote_apply incurred while committing if this server was configured as a synchronous standby.", nil, semver.MustParseRange(">=10.0.0")},
	},
	"pg_stat_activity": {
		"datname":         {LABEL, "Name of this database", nil, nil},
		"state":           {LABEL, "connection state", nil, semver.MustParseRange(">=9.2.0")},
		"count":           {GAUGE, "number of connections in this state", nil, nil},
		"max_tx_duration": {GAUGE, "max duration in seconds any active transaction has been running", nil, nil},
	},
}

// OverrideQuery 's are run in-place of simple namespace look ups, and provide
// advanced functionality. But they have a tendency to postgres version specific.
// There aren't too many versions, so we simply store customized versions using
// the semver matching we do for columns.
type OverrideQuery struct {
	versionRange semver.Range
	query        string
}

// Overriding queries for namespaces above.
// TODO: validate this is a closed set in tests, and there are no overlaps
var queryOverrides = map[string][]OverrideQuery{
	"pg_locks": {
		{
			semver.MustParseRange(">0.0.0"),
			`SELECT pg_database.datname,tmp.mode,COALESCE(count,0) as count
			FROM
				(
				  VALUES ('accesssharelock'),
				         ('rowsharelock'),
				         ('rowexclusivelock'),
				         ('shareupdateexclusivelock'),
				         ('sharelock'),
				         ('sharerowexclusivelock'),
				         ('exclusivelock'),
				         ('accessexclusivelock')
				) AS tmp(mode) CROSS JOIN pg_database
			LEFT JOIN
			  (SELECT database, lower(mode) AS mode,count(*) AS count
			  FROM pg_locks WHERE database IS NOT NULL
			  GROUP BY database, lower(mode)
			) AS tmp2
			ON tmp.mode=tmp2.mode and pg_database.oid = tmp2.database ORDER BY 1`,
		},
	},

	"pg_stat_replication": {
		{
			semver.MustParseRange(">=10.0.0"),
			`
			SELECT *,
				(case pg_is_in_recovery() when 't' then null else pg_current_wal_lsn() end) AS pg_current_wal_lsn,
				(case pg_is_in_recovery() when 't' then null else pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::float end) AS pg_wal_lsn_diff
			FROM pg_stat_replication
			`,
		},
		{
			semver.MustParseRange(">=9.2.0 <10.0.0"),
			`
			SELECT *,
				(case pg_is_in_recovery() when 't' then null else pg_current_xlog_location() end) AS pg_current_xlog_location,
				(case pg_is_in_recovery() when 't' then null else pg_xlog_location_diff(pg_current_xlog_location(), replay_location)::float end) AS pg_xlog_location_diff
			FROM pg_stat_replication
			`,
		},
		{
			semver.MustParseRange("<9.2.0"),
			`
			SELECT *,
				(case pg_is_in_recovery() when 't' then null else pg_current_xlog_location() end) AS pg_current_xlog_location
			FROM pg_stat_replication
			`,
		},
	},

	"pg_stat_activity": {
		// This query only works
		{
			semver.MustParseRange(">=9.2.0"),
			`
			SELECT
				pg_database.datname,
				tmp.state,
				COALESCE(count,0) as count,
				COALESCE(max_tx_duration,0) as max_tx_duration
			FROM
				(
				  VALUES ('active'),
				  		 ('idle'),
				  		 ('idle in transaction'),
				  		 ('idle in transaction (aborted)'),
				  		 ('fastpath function call'),
				  		 ('disabled')
				) AS tmp(state) CROSS JOIN pg_database
			LEFT JOIN
			(
				SELECT
					datname,
					state,
					count(*) AS count,
					MAX(EXTRACT(EPOCH FROM now() - xact_start))::float AS max_tx_duration
				FROM pg_stat_activity GROUP BY datname,state) AS tmp2
				ON tmp.state = tmp2.state AND pg_database.datname = tmp2.datname
			`,
		},
		// No query is applicable for 9.1 that gives any sensible data.
	},
}

// Convert the query override file to the version-specific query override file
// for the exporter.
func makeQueryOverrideMap(pgVersion semver.Version, queryOverrides map[string][]OverrideQuery) map[string]string {
	resultMap := make(map[string]string)
	for name, overrideDef := range queryOverrides {
		// Find a matching semver. We make it an error to have overlapping
		// ranges at test-time, so only 1 should ever match.
		matched := false
		for _, queryDef := range overrideDef {
			if queryDef.versionRange(pgVersion) {
				resultMap[name] = queryDef.query
				matched = true
				break
			}
		}
		if !matched {
			log.Warnln("No query matched override for", name, "- disabling metric space.")
			resultMap[name] = ""
		}
	}

	return resultMap
}

// Add queries to the builtinMetricMaps and queryOverrides maps. Added queries do not
// respect version requirements, because it is assumed that the user knows
// what they are doing with their version of postgres.
//
// This function modifies metricMap and queryOverrideMap to contain the new
// queries.
// TODO: test code for all cu.
// TODO: use proper struct type system
// TODO: the YAML this supports is "non-standard" - we should move away from it.
func addQueries(content []byte, pgVersion semver.Version, exporterMap map[string]MetricMapNamespace, queryOverrideMap map[string]string) error {
	var extra map[string]interface{}

	err := yaml.Unmarshal(content, &extra)
	if err != nil {
		return err
	}

	// Stores the loaded map representation
	metricMaps := make(map[string]map[string]ColumnMapping)
	newQueryOverrides := make(map[string]string)

	for metric, specs := range extra {
		log.Debugln("New user metric namespace from YAML:", metric)
		for key, value := range specs.(map[interface{}]interface{}) {
			switch key.(string) {
			case "query":
				query := value.(string)
				newQueryOverrides[metric] = query

			case "metrics":
				for _, c := range value.([]interface{}) {
					column := c.(map[interface{}]interface{})

					for n, a := range column {
						var columnMapping ColumnMapping

						// Fetch the metric map we want to work on.
						metricMap, ok := metricMaps[metric]
						if !ok {
							// Namespace for metric not found - add it.
							metricMap = make(map[string]ColumnMapping)
							metricMaps[metric] = metricMap
						}

						// Get name.
						name := n.(string)

						for attrKey, attrVal := range a.(map[interface{}]interface{}) {
							switch attrKey.(string) {
							case "usage":
								usage, err := stringToColumnUsage(attrVal.(string))
								if err != nil {
									return err
								}
								columnMapping.usage = usage
							case "description":
								columnMapping.description = attrVal.(string)
							}
						}

						// TODO: we should support cu
						columnMapping.mapping = nil
						// Should we support this for users?
						columnMapping.supportedVersions = nil

						metricMap[name] = columnMapping
					}
				}
			}
		}
	}

	// Convert the loaded metric map into exporter representation
	partialExporterMap := makeDescMap(pgVersion, metricMaps)

	// Merge the two maps (which are now quite flatteend)
	for k, v := range partialExporterMap {
		_, found := exporterMap[k]
		if found {
			log.Debugln("Overriding metric", k, "from user YAML file.")
		} else {
			log.Debugln("Adding new metric", k, "from user YAML file.")
		}
		exporterMap[k] = v
	}

	// Merge the query override map
	for k, v := range newQueryOverrides {
		_, found := queryOverrideMap[k]
		if found {
			log.Debugln("Overriding query override", k, "from user YAML file.")
		} else {
			log.Debugln("Adding new query override", k, "from user YAML file.")
		}
		queryOverrideMap[k] = v
	}

	return nil
}

// Turn the MetricMap column mapping into a prometheus descriptor mapping.
func makeDescMap(pgVersion semver.Version, metricMaps map[string]map[string]ColumnMapping) map[string]MetricMapNamespace {
	var metricMap = make(map[string]MetricMapNamespace)

	for namespace, mappings := range metricMaps {
		thisMap := make(map[string]MetricMap)

		// Get the constant labels
		var constLabels []string
		for columnName, columnMapping := range mappings {
			if columnMapping.usage == LABEL {
				constLabels = append(constLabels, columnName)
			}
		}

		for columnName, columnMapping := range mappings {
			// Check column version compatibility for the current map
			// Force to discard if not compatible.
			if columnMapping.supportedVersions != nil {
				if !columnMapping.supportedVersions(pgVersion) {
					// It's very useful to be able to see what columns are being
					// rejected.
					log.Debugln(columnName, "is being forced to discard due to version incompatibility.")
					thisMap[columnName] = MetricMap{
						discard: true,
						conversion: func(_ interface{}) (float64, bool) {
							return math.NaN(), true
						},
					}
					continue
				}
			}

			// Determine how to convert the column based on its usage.
			// nolint: dupl
			switch columnMapping.usage {
			case DISCARD, LABEL:
				thisMap[columnName] = MetricMap{
					discard: true,
					conversion: func(_ interface{}) (float64, bool) {
						return math.NaN(), true
					},
				}
			case COUNTER:
				thisMap[columnName] = MetricMap{
					vtype: prometheus.CounterValue,
					desc:  prometheus.NewDesc(fmt.Sprintf("%s_%s", namespace, columnName), columnMapping.description, constLabels, nil),
					conversion: func(in interface{}) (float64, bool) {
						return dbToFloat64(in)
					},
				}
			case GAUGE:
				thisMap[columnName] = MetricMap{
					vtype: prometheus.GaugeValue,
					desc:  prometheus.NewDesc(fmt.Sprintf("%s_%s", namespace, columnName), columnMapping.description, constLabels, nil),
					conversion: func(in interface{}) (float64, bool) {
						return dbToFloat64(in)
					},
				}
			case MAPPEDMETRIC:
				thisMap[columnName] = MetricMap{
					vtype: prometheus.GaugeValue,
					desc:  prometheus.NewDesc(fmt.Sprintf("%s_%s", namespace, columnName), columnMapping.description, constLabels, nil),
					conversion: func(in interface{}) (float64, bool) {
						text, ok := in.(string)
						if !ok {
							return math.NaN(), false
						}

						val, ok := columnMapping.mapping[text]
						if !ok {
							return math.NaN(), false
						}
						return val, true
					},
				}
			case DURATION:
				thisMap[columnName] = MetricMap{
					vtype: prometheus.GaugeValue,
					desc:  prometheus.NewDesc(fmt.Sprintf("%s_%s_milliseconds", namespace, columnName), columnMapping.description, constLabels, nil),
					conversion: func(in interface{}) (float64, bool) {
						var durationString string
						switch t := in.(type) {
						case []byte:
							durationString = string(t)
						case string:
							durationString = t
						default:
							log.Errorln("DURATION conversion metric was not a string")
							return math.NaN(), false
						}

						if durationString == "-1" {
							return math.NaN(), false
						}

						d, err := time.ParseDuration(durationString)
						if err != nil {
							log.Errorln("Failed converting result to metric:", columnName, in, err)
							return math.NaN(), false
						}
						return float64(d / time.Millisecond), true
					},
				}
			}
		}

		metricMap[namespace] = MetricMapNamespace{constLabels, thisMap}
	}

	return metricMap
}

// convert a string to the corresponding ColumnUsage
func stringToColumnUsage(s string) (ColumnUsage, error) {
	var u ColumnUsage
	var err error
	switch s {
	case "DISCARD":
		u = DISCARD

	case "LABEL":
		u = LABEL

	case "COUNTER":
		u = COUNTER

	case "GAUGE":
		u = GAUGE

	case "MAPPEDMETRIC":
		u = MAPPEDMETRIC

	case "DURATION":
		u = DURATION

	default:
		err = fmt.Errorf("wrong ColumnUsage given : %s", s)
	}

	return u, err
}

// Convert database.sql types to float64s for Prometheus consumption. Null types are mapped to NaN. string and []byte
// types are mapped as NaN and !ok
func dbToFloat64(t interface{}) (float64, bool) {
	switch v := t.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case time.Time:
		return float64(v.Unix()), true
	case []byte:
		// Try and convert to string and then parse to a float64
		strV := string(v)
		result, err := strconv.ParseFloat(strV, 64)
		if err != nil {
			log.Infoln("Could not parse []byte:", err)
			return math.NaN(), false
		}
		return result, true
	case string:
		result, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Infoln("Could not parse string:", err)
			return math.NaN(), false
		}
		return result, true
	case nil:
		return math.NaN(), true
	default:
		return math.NaN(), false
	}
}

// Convert database.sql to string for Prometheus labels. Null types are mapped to empty strings.
func dbToString(t interface{}) (string, bool) {
	switch v := t.(type) {
	case int64:
		return fmt.Sprintf("%v", v), true
	case float64:
		return fmt.Sprintf("%v", v), true
	case time.Time:
		return fmt.Sprintf("%v", v.Unix()), true
	case nil:
		return "", true
	case []byte:
		// Try and convert to string
		return string(v), true
	case string:
		return v, true
	default:
		return "", false
	}
}

// Exporter collects Postgres metrics. It implements prometheus.Collector.
type Exporter struct {
	// Holds a reference to the build in column mappings. Currently this is for testing purposes
	// only, since it just points to the global.
	builtinMetricMaps map[string]map[string]ColumnMapping

	dsn                   string
	disableDefaultMetrics bool
	userQueriesPath       string
	duration              prometheus.Gauge
	error                 prometheus.Gauge
	psqlUp                prometheus.Gauge
	userQueriesError      *prometheus.GaugeVec
	totalScrapes          prometheus.Counter

	// dbDsn is the connection string used to establish the dbConnection
	dbDsn string
	// dbConnection is used to allow re-using the DB connection between scrapes
	dbConnection *sql.DB

	// Last version used to calculate metric map. If mismatch on scrape,
	// then maps are recalculated.
	lastMapVersion semver.Version
	// Currently active metric map
	metricMap map[string]MetricMapNamespace
	// Currently active query overrides
	queryOverrides map[string]string
	mappingMtx     sync.RWMutex
}

// NewExporter returns a new PostgreSQL exporter for the provided DSN.
func NewExporter(dsn string, disableDefaultMetrics bool, userQueriesPath string) *Exporter {
	return &Exporter{
		builtinMetricMaps:     builtinMetricMaps,
		dsn:                   dsn,
		disableDefaultMetrics: disableDefaultMetrics,
		userQueriesPath:       userQueriesPath,
		duration: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_duration_seconds",
			Help:      "Duration of the last scrape of metrics from PostgresSQL.",
		}),
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "scrapes_total",
			Help:      "Total number of times PostgresSQL was scraped for metrics.",
		}),
		error: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "last_scrape_error",
			Help:      "Whether the last scrape of metrics from PostgreSQL resulted in an error (1 for error, 0 for success).",
		}),
		psqlUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Whether the last scrape of metrics from PostgreSQL was able to connect to the server (1 for yes, 0 for no).",
		}),
		userQueriesError: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: exporter,
			Name:      "user_queries_load_error",
			Help:      "Whether the user queries file was loaded and parsed successfully (1 for error, 0 for success).",
		}, []string{"filename", "hashsum"}),
		metricMap:      nil,
		queryOverrides: nil,
	}
}

// Describe implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	// We cannot know in advance what metrics the exporter will generate
	// from Postgres. So we use the poor man's describe method: Run a collect
	// and send the descriptors of all the collected metrics. The problem
	// here is that we need to connect to the Postgres DB. If it is currently
	// unavailable, the descriptors will be incomplete. Since this is a
	// stand-alone exporter and not used as a library within other code
	// implementing additional metrics, the worst that can happen is that we
	// don't detect inconsistent metrics created by this exporter
	// itself. Also, a change in the monitored Postgres instance may change the
	// exported metrics during the runtime of the exporter.

	metricCh := make(chan prometheus.Metric)
	doneCh := make(chan struct{})

	go func() {
		for m := range metricCh {
			ch <- m.Desc()
		}
		close(doneCh)
	}()

	e.Collect(metricCh)
	close(metricCh)
	<-doneCh
}

// Collect implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.scrape(ch)

	ch <- e.duration
	ch <- e.totalScrapes
	ch <- e.error
	ch <- e.psqlUp
	e.userQueriesError.Collect(ch)
}

func newDesc(subsystem, name, help string) *prometheus.Desc {
	return prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, name),
		help, nil, nil,
	)
}

// Query within a namespace mapping and emit metrics. Returns fatal errors if
// the scrape fails, and a slice of errors if they were non-fatal.
func queryNamespaceMapping(ch chan<- prometheus.Metric, db *sql.DB, namespace string, mapping MetricMapNamespace, queryOverrides map[string]string) ([]error, error) {
	// Check for a query override for this namespace
	query, found := queryOverrides[namespace]

	// Was this query disabled (i.e. nothing sensible can be queried on cu
	// version of PostgreSQL?
	if query == "" && found {
		// Return success (no pertinent data)
		return []error{}, nil
	}

	// Don't fail on a bad scrape of one metric
	var rows *sql.Rows
	var err error

	if !found {
		// I've no idea how to avoid this properly at the moment, but this is
		// an admin tool so you're not injecting SQL right?
		rows, err = db.Query(fmt.Sprintf("SELECT * FROM %s;", namespace)) // nolint: gas, safesql
	} else {
		rows, err = db.Query(query) // nolint: safesql
	}
	if err != nil {
		return []error{}, errors.New(fmt.Sprintln("Error running query on database: ", namespace, err))
	}
	defer rows.Close() // nolint: errcheck

	var columnNames []string
	columnNames, err = rows.Columns()
	if err != nil {
		return []error{}, errors.New(fmt.Sprintln("Error retrieving column list for: ", namespace, err))
	}

	// Make a lookup map for the column indices
	var columnIdx = make(map[string]int, len(columnNames))
	for i, n := range columnNames {
		columnIdx[n] = i
	}

	var columnData = make([]interface{}, len(columnNames))
	var scanArgs = make([]interface{}, len(columnNames))
	for i := range columnData {
		scanArgs[i] = &columnData[i]
	}

	nonfatalErrors := []error{}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return []error{}, errors.New(fmt.Sprintln("Error retrieving rows:", namespace, err))
		}

		// Get the label values for this row
		var labels = make([]string, len(mapping.labels))
		for idx, columnName := range mapping.labels {
			labels[idx], _ = dbToString(columnData[columnIdx[columnName]])
		}

		// Loop over column names, and match to scan data. Unknown columns
		// will be filled with an untyped metric number *if* they can be
		// converted to float64s. NULLs are allowed and treated as NaN.
		for idx, columnName := range columnNames {
			if metricMapping, ok := mapping.columnMappings[columnName]; ok {
				// Is this a metricy metric?
				if metricMapping.discard {
					continue
				}

				value, ok := dbToFloat64(columnData[idx])
				if !ok {
					nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unexpected error parsing column: ", namespace, columnName, columnData[idx])))
					continue
				}

				// Generate the metric
				ch <- prometheus.MustNewConstMetric(metricMapping.desc, metricMapping.vtype, value, labels...)
			} else {
				// Unknown metric. Report as untyped if scan to float64 works, else note an error too.
				metricLabel := fmt.Sprintf("%s_%s", namespace, columnName)
				desc := prometheus.NewDesc(metricLabel, fmt.Sprintf("Unknown metric from %s", namespace), mapping.labels, nil)

				// Its not an error to fail here, since the values are
				// unexpected anyway.
				value, ok := dbToFloat64(columnData[idx])
				if !ok {
					nonfatalErrors = append(nonfatalErrors, errors.New(fmt.Sprintln("Unparseable column type - discarding: ", namespace, columnName, err)))
					continue
				}
				ch <- prometheus.MustNewConstMetric(desc, prometheus.UntypedValue, value, labels...)
			}
		}
	}
	return nonfatalErrors, nil
}

// Iterate through all the namespace mappings in the exporter and run their
// queries.
func queryNamespaceMappings(ch chan<- prometheus.Metric, db *sql.DB, metricMap map[string]MetricMapNamespace, queryOverrides map[string]string) map[string]error {
	// Return a map of namespace -> errors
	namespaceErrors := make(map[string]error)

	for namespace, mapping := range metricMap {
		log.Debugln("Querying namespace: ", namespace)
		nonFatalErrors, err := queryNamespaceMapping(ch, db, namespace, mapping, queryOverrides)
		// Serious error - a namespace disappeared
		if err != nil {
			namespaceErrors[namespace] = err
			log.Infoln(err)
		}
		// Non-serious errors - likely version or parsing problems.
		if len(nonFatalErrors) > 0 {
			for _, err := range nonFatalErrors {
				log.Infoln(err.Error())
			}
		}
	}

	return namespaceErrors
}

// Check and update the exporters query maps if the version has changed.
func (e *Exporter) checkMapVersions(ch chan<- prometheus.Metric, db *sql.DB) error {
	log.Debugln("Querying Postgres Version")
	versionRow := db.QueryRow("SELECT version();")
	var versionString string
	err := versionRow.Scan(&versionString)
	if err != nil {
		return fmt.Errorf("Error scanning version string: %v", err)
	}
	semanticVersion, err := parseVersion(versionString)
	if err != nil {
		return fmt.Errorf("Error parsing version string: %v", err)
	}
	if !e.disableDefaultMetrics && semanticVersion.LT(lowestSupportedVersion) {
		log.Warnln("PostgreSQL version is lower then our lowest supported version! Got", semanticVersion.String(), "minimum supported is", lowestSupportedVersion.String())
	}

	// Check if semantic version changed and recalculate maps if needed.
	if semanticVersion.NE(e.lastMapVersion) || e.metricMap == nil {
		log.Infoln("Semantic Version Changed:", e.lastMapVersion.String(), "->", semanticVersion.String())
		e.mappingMtx.Lock()

		if e.disableDefaultMetrics {
			e.metricMap = make(map[string]MetricMapNamespace)
		} else {
			e.metricMap = makeDescMap(semanticVersion, e.builtinMetricMaps)
		}

		if e.disableDefaultMetrics {
			e.queryOverrides = make(map[string]string)
		} else {
			e.queryOverrides = makeQueryOverrideMap(semanticVersion, queryOverrides)
		}

		e.lastMapVersion = semanticVersion

		if e.userQueriesPath != "" {
			// Clear the metric while a reload is happening
			e.userQueriesError.Reset()

			// Calculate the hashsum of the useQueries
			userQueriesData, err := ioutil.ReadFile(e.userQueriesPath)
			if err != nil {
				log.Errorln("Failed to reload user queries:", e.userQueriesPath, err)
				e.userQueriesError.WithLabelValues(e.userQueriesPath, "").Set(1)
			} else {
				hashsumStr := fmt.Sprintf("%x", sha256.Sum256(userQueriesData))

				if err := addQueries(userQueriesData, semanticVersion, e.metricMap, e.queryOverrides); err != nil {
					log.Errorln("Failed to reload user queries:", e.userQueriesPath, err)
					e.userQueriesError.WithLabelValues(e.userQueriesPath, hashsumStr).Set(1)
				} else {
					// Mark user queries as successfully loaded
					e.userQueriesError.WithLabelValues(e.userQueriesPath, hashsumStr).Set(0)
				}
			}
		}

		e.mappingMtx.Unlock()
	}

	// Output the version as a special metric
	versionDesc := prometheus.NewDesc(fmt.Sprintf("%s_%s", namespace, staticLabelName),
		"Version string as reported by postgres", []string{"version", "short_version"}, nil)

	ch <- prometheus.MustNewConstMetric(versionDesc,
		prometheus.UntypedValue, 1, versionString, semanticVersion.String())
	return nil
}

func (e *Exporter) getDB(conn string) (*sql.DB, error) {
	// Has dsn changed?
	if (e.dbConnection != nil) && (e.dsn != e.dbDsn) {
		err := e.dbConnection.Close()
		log.Warnln("Error while closing obsolete DB connection:", err)
		e.dbConnection = nil
		e.dbDsn = ""
	}

	if e.dbConnection == nil {
		d, err := sql.Open("postgres", conn)
		if err != nil {
			return nil, err
		}

		d.SetMaxOpenConns(1)
		d.SetMaxIdleConns(1)
		e.dbConnection = d
		e.dbDsn = e.dsn
		log.Infoln("Established new database connection.")
	}

	// Always send a ping and possibly invalidate the connection if it fails
	if err := e.dbConnection.Ping(); err != nil {
		cerr := e.dbConnection.Close()
		log.Infoln("Error while closing non-pinging DB connection:", cerr)
		e.dbConnection = nil
		e.psqlUp.Set(0)
		return nil, err
	}

	return e.dbConnection, nil
}

func (e *Exporter) scrape(ch chan<- prometheus.Metric) {
	defer func(begun time.Time) {
		e.duration.Set(time.Since(begun).Seconds())
	}(time.Now())

	e.error.Set(0)
	e.totalScrapes.Inc()

	db, err := e.getDB(e.dsn)
	if err != nil {
		loggableDsn := "could not parse DATA_SOURCE_NAME"
		// If the DSN is parseable, log it with a blanked out password
		pDsn, pErr := url.Parse(e.dsn)
		if pErr == nil {
			// Blank user info if not nil
			if pDsn.User != nil {
				pDsn.User = url.UserPassword(pDsn.User.Username(), "PASSWORD_REMOVED")
			}
			loggableDsn = pDsn.String()
		}
		log.Infof("Error opening connection to database (%s): %s", loggableDsn, err)
		e.psqlUp.Set(0)
		e.error.Set(1)
		return
	}

	// Didn't fail, can mark connection as up for this scrape.
	e.psqlUp.Set(1)

	// Check if map versions need to be updated
	if err := e.checkMapVersions(ch, db); err != nil {
		log.Warnln("Proceeding with outdated query maps, as the Postgres version could not be determined:", err)
		e.error.Set(1)
	}

	// Lock the exporter maps
	e.mappingMtx.RLock()
	defer e.mappingMtx.RUnlock()
	if err := querySettings(ch, db); err != nil {
		log.Infof("Error retrieving settings: %s", err)
		e.error.Set(1)
	}

	errMap := queryNamespaceMappings(ch, db, e.metricMap, e.queryOverrides)
	if len(errMap) > 0 {
		e.error.Set(1)
	}
}

func getDataSource() string {
	var dsn = os.Getenv("DATA_SOURCE_NAME")
	if dsn == "" {
		dsn = lookupConfig("dsn", "").(string)
	}
	if len(dsn) == 0 {
		var user string
		var pass string

		if len(os.Getenv("DATA_SOURCE_USER_FILE")) != 0 {
			fileContents, err := ioutil.ReadFile(os.Getenv("DATA_SOURCE_USER_FILE"))
			if err != nil {
				panic(err)
			}
			user = strings.TrimSpace(string(fileContents))
		} else {
			user = os.Getenv("DATA_SOURCE_USER")
		}

		if len(os.Getenv("DATA_SOURCE_PASS_FILE")) != 0 {
			fileContents, err := ioutil.ReadFile(os.Getenv("DATA_SOURCE_PASS_FILE"))
			if err != nil {
				panic(err)
			}
			pass = strings.TrimSpace(string(fileContents))
		} else {
			pass = os.Getenv("DATA_SOURCE_PASS")
		}

		ui := url.UserPassword(user, pass).String()
		uri := os.Getenv("DATA_SOURCE_URI")
		dsn = "postgresql://" + ui + "@" + uri
	}

	return dsn
}

func getStringEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getBoolEnv(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		v, _ := strconv.ParseBool(value)
		return v
	}
	return fallback
}

var cfg = new(config)

func main() {
	// Parse flags.
	flag.Parse()

	log.Infoln("Starting postgres_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	if *showVersion {
		fmt.Fprintln(os.Stdout, version.Print("postgres_exporter"))
		os.Exit(0)
	}

	if os.Getenv("ON_CONFIGURE") == "1" {
		err := configure()
		if err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	}

	err := ini.MapTo(cfg, *configPath)
	if err != nil {
		log.Fatal(fmt.Sprintf("Load config file %s failed: %s", *configPath, err.Error()))
	}

	// set flags for exporter_shared server
	flag.Set("web.ssl-cert-file", lookupConfig("web.ssl-cert-file", "").(string))
	flag.Set("web.ssl-key-file", lookupConfig("web.ssl-key-file", "").(string))
	flag.Set("web.auth-file", lookupConfig("web.auth-file", "/opt/ss/ssm-client/ssm.yml").(string))

	if lookupConfig("dumpmaps", *onlyDumpMaps).(bool) {
		dumpMaps()
		return
	}

	dsn := getDataSource()
	if len(dsn) == 0 {
		log.Fatal("couldn't find environment variables describing the datasource to use")
	}

	exporter := NewExporter(dsn, lookupConfig("disable-default-metrics", *disableDefaultMetrics).(bool), lookupConfig("query-path", *queriesPath).(string))
	defer func() {
		if exporter.dbConnection != nil {
			exporter.dbConnection.Close() // nolint: errcheck
		}
	}()

	prometheus.MustRegister(exporter)

	// Use our shared code to run server and exit on error. Upstream's code below will not be executed.
	exporter_shared.RunServer("PostgreSQL", lookupConfig("web.listen-address", *listenAddress).(string), lookupConfig("web.telemetry-path", *metricsPath).(string), promhttp.ContinueOnError)
}

type config struct {
	DSN                   string       `ini:"dsn"`
	DisableDefaultMetrics bool         `ini:"disable-default-metrics"`
	Dumpmaps              bool         `ini:"dumpmaps"`
	Web                   webConfig    `ini:"web"`
	Extend                extendConfig `ini:"extend"`
}

type webConfig struct {
	ListenAddress string  `ini:"listen-address"`
	MetricsPath   string  `ini:"telemetry-path"`
	SSLCertFile   string  `ini:"ssl-cert-file"`
	SSLKeyFile    string  `ini:"ssl-key-file"`
	AuthFile      *string `ini:"auth-file"`
}

type extendConfig struct {
	QueryPath string `ini:"query-path"`
}

// lookupConfig lookup config from flag
// or config by name, returns nil if none exists.
// name should be in this format -> '[section].[key]'
func lookupConfig(name string, defaultValue interface{}) interface{} {
	flagSet, flagValue := lookupFlag(name)
	if flagSet {
		return flagValue
	}

	section := ""
	key := name
	if i := strings.Index(name, "."); i > 0 {
		section = name[0:i]
		if len(name) > i+1 {
			key = name[i+1:]
		} else {
			key = ""
		}
	}

	t := reflect.TypeOf(*cfg)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		iniName := field.Tag.Get("ini")
		matched := iniName == section
		if section == "" {
			matched = iniName == key
		}
		if !matched {
			continue
		}

		v := reflect.ValueOf(cfg).Elem().Field(i)
		if section == "" {
			return v.Interface()
		}

		if !v.CanAddr() {
			continue
		}

		st := reflect.TypeOf(v.Interface())
		for j := 0; j < st.NumField(); j++ {
			sectionField := st.Field(j)
			sectionININame := sectionField.Tag.Get("ini")
			if sectionININame != key {
				continue
			}

			if reflect.ValueOf(v.Addr().Elem().Field(j).Interface()).Kind() != reflect.Ptr {
				return v.Addr().Elem().Field(j).Interface()
			}

			if v.Addr().Elem().Field(j).IsNil() {
				return defaultValue
			}

			return v.Addr().Elem().Field(j).Elem().Interface()
		}
	}

	return defaultValue
}

func lookupFlag(name string) (flagSet bool, flagValue interface{}) {
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			flagSet = true
			switch reflect.Indirect(reflect.ValueOf(f.Value)).Kind() {
			case reflect.Bool:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).Bool()
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).Int()
			case reflect.Float32, reflect.Float64:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).Float()
			case reflect.String:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).String()
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				flagValue = reflect.Indirect(reflect.ValueOf(f.Value)).Uint()
			}
		}
	})

	return
}

func configure() error {
	iniCfg, err := ini.Load(*configPath)
	if err != nil {
		return err
	}

	if err = iniCfg.MapTo(cfg); err != nil {
		return err
	}

	type item struct {
		value   reflect.Value
		section string
	}

	items := []item{
		{
			value:   reflect.ValueOf(cfg).Elem(),
			section: "",
		},
	}
	for i := 0; i < len(items); i++ {
		for j := 0; j < items[i].value.Type().NumField(); j++ {
			fieldValue := items[i].value.Field(j)
			fieldType := items[i].value.Type().Field(j)
			section := items[i].section
			key := fieldType.Tag.Get("ini")

			if fieldValue.Kind() == reflect.Struct {
				if fieldValue.CanAddr() && section == "" {
					items = append(items, item{
						value:   fieldValue.Addr().Elem(),
						section: key,
					})
				}
				continue
			}

			flagSet, flagValue := lookupFlag(fmt.Sprintf("%s.%s", section, key))
			if !flagSet {
				continue
			}

			if fieldValue.IsValid() && fieldValue.CanSet() {
				switch fieldValue.Kind() {
				case reflect.Bool:
					iniCfg.Section(section).Key(key).SetValue(fmt.Sprintf("%t", flagValue.(bool)))
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					iniCfg.Section(section).Key(key).SetValue(fmt.Sprintf("%d", flagValue.(int64)))
				case reflect.Float32, reflect.Float64:
					iniCfg.Section(section).Key(key).SetValue(fmt.Sprintf("%f", flagValue.(float64)))
				case reflect.String:
					iniCfg.Section(section).Key(key).SetValue(strconv.Quote(flagValue.(string)))
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					iniCfg.Section(section).Key(key).SetValue(fmt.Sprintf("%d", flagValue.(uint64)))
				}
			}
		}
	}

	if os.Getenv("DATA_SOURCE_NAME") != "" {
		iniCfg.Section("").Key("dsn").SetValue(strconv.Quote(os.Getenv("DATA_SOURCE_NAME")))
	}

	if err = iniCfg.SaveTo(*configPath); err != nil {
		return err
	}

	return nil
}
