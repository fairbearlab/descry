// Package metrics is a Prometheus collector for programs that embed the
// descry engine; the descry CLI serves it with --listen. It instruments
// a check.Check with a decorator (Collector.Check), reads the runner's exact
// skip and drop counters through the small RunnerStats interface (attached
// with Collector.SetRunner once the runner exists), and serves everything
// from its own *prometheus.Registry (Collector.Registry). It never touches
// prometheus.DefaultRegisterer, and it registers no Go runtime or process
// collectors: the registry holds descry_* series only, so a caller that wants
// those adds them to the registry itself.
//
// # Series
//
// Per-target family (label url = TargetLabel(target.URL); dropped entirely
// with Options.DisablePerTarget, which the CLI's --metrics-per-target=false
// sets):
//
//	descry_check_up                           gauge    {url}         1 when the last run was check.StatusUp, else 0
//	descry_check_status_code                  gauge    {url}         last HTTP status code; 0 with no response or a Run error
//	descry_check_latency_seconds              gauge    {url}         last run's latency (the last value, not a histogram)
//	descry_check_last_run_timestamp_seconds   gauge    {url}         Observation.ObservedAt, unix seconds
//	descry_check_tls_expiry_timestamp_seconds gauge    {url}         expiry of the final response's certificate (after redirects), unix seconds
//	descry_check_runs_total                   counter  {url,status}  runs, status "up" or "down"; a Run error counts as down
//
// Fleet series (always registered):
//
//	descry_check_duration_seconds        histogram         latency of every completed Observation
//	descry_check_errors_total            counter  {class}  failed Observations by check.ErrorClass; Run errors count as unknown
//	descry_runner_skipped_slots_total    counter           RunnerStats.Skipped(), both skip kinds, exact
//	descry_runner_dropped_results_total  counter           RunnerStats.Dropped(), exact
//	descry_runner_result_errors_total    counter           Options.ResultErrors(), non-skip errors, best-effort
//	descry_runner_skips_observed_total   counter  {kind}   skips seen on Results(), best-effort
//	descry_runner_targets                gauge             SetTargetCount
//	descry_build_info                    gauge    {version,commit}  constant 1
//
// Target.Labels and Observation.Labels are never exported: they are opaque
// consumer data with unbounded cardinality and may carry values that must not
// reach an unauthenticated scrape endpoint.
//
// # Target labels
//
// The url label is TargetLabel(target.URL): scheme://host[:port]/path, with
// userinfo, query and fragment always dropped, and an empty path rendered as
// "/". The path is exported as-is, so a URL that carries a secret in its path
// (a tokenised webhook or capability URL) should not be monitored with the
// per-target family; set Options.DisablePerTarget for such fleets. Targets
// that differ only in the dropped parts share one series set: their counters
// sum and their gauges are last-writer-wins. Every target whose URL has no
// safe rendering shares the single url="<unparseable>" series set. That
// includes any URL with a literal "@" besides the userinfo delimiter, which is
// how a password with an unescaped "/", "?" or "#" shows up once url.Parse has
// misread it; a literal "@" in a legitimate path, query or fragment (an email
// in a query parameter, say) gets the placeholder too unless written as "%40".
// The CLI refuses to start (exit 2) when two targets share a label and
// per-target series are served; the library itself does not check.
//
// # Skips
//
// descry_runner_skipped_slots_total is the exact count of skipped slots; alert
// on it. descry_runner_skips_observed_total{kind} is triage only: it is fed by
// ObserveSkip from the single Results() drain, so it undercounts whenever the
// runner drops Results under saturation. Once the drain has caught up,
// descry_runner_dropped_results_total bounds that gap (skipped - observed <=
// dropped); a live scrape can lag further by the Results still buffered in the
// channel (up to about 2x the target count), so do not alert on the inequality.
// descry_runner_result_errors_total has the same gap: it counts only the error
// Results the drain receives.
//
// # Wiring
//
// The order is fixed by runner.New taking the decorated check:
//
//	c, err := metrics.New(metrics.Options{ResultErrors: failures.Load})
//	r := runner.New(c.Check(inner), sink, evCfg, targets, interval, concurrency)
//	c.SetRunner(r)
//	c.SetTargetCount(len(targets))
//
// and the single Results() drain reports skips with ObserveSkip, testing
// runner.ErrSkippedQueued before runner.ErrSkipped, and counts the other
// error Results in whatever Options.ResultErrors reads.
//
// The runner series read 0 until they are wired: skipped_slots_total and
// dropped_results_total until SetRunner, result_errors_total without
// Options.ResultErrors, and runner_targets until SetTargetCount. A constant 0
// on one of them can mean an unwired collector rather than a healthy runner.
//
// # Limitations
//
// Removing a target at runtime is not handled. Target sets are static today;
// a removed target's series would stay exported until the process restarts.
package metrics
