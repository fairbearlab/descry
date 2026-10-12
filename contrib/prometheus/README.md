# Prometheus alerting rules

`descry-rules.yaml` is a [Prometheus Operator](https://prometheus-operator.dev/)
`PrometheusRule` with one group, `descry`, and six alerts over the series that
`descry --listen` serves (or that a program embedding the `metrics` package
exports):

| Alert | Fires when | Severity |
|---|---|---|
| `DescryProberAbsent` | no instance has reported `descry_build_info` for 5m | critical |
| `DescryTargetDown` | a target's `descry_check_up` has been 0 for 5m (needs the per-target series, `--metrics-per-target`, on by default) | critical |
| `DescryChecksStale` | a target's `descry_check_last_run_timestamp_seconds` is more than 15m old (needs the per-target series) | critical |
| `DescrySkipsClimbing` | the exact `descry_runner_skipped_slots_total` rose at least once in every 10m window for 15m | warning |
| `DescryResultsDropped` | `descry_runner_dropped_results_total` rose in the last 10m (a dropped `Result` can hide a failed check or publish; its diagnostics and the counts fed from `Results()` are lost) | warning |
| `DescryPublishErrors` | `descry_runner_result_errors_total` rose in the last 10m (best-effort series) | warning |

The skip alert reads the exact total, not `descry_runner_skips_observed_total`,
which is for triage only. The series and their limits are in
[docs/OPERATIONS.md](../../docs/OPERATIONS.md#metrics-and-health-endpoints) and
the `metrics` package doc. To run descry itself in a cluster, see the
[Kubernetes example](../../examples/kubernetes/README.md); its pod serves the
series these rules read, but something still has to scrape it (see below).
Its config also has a target that is down on purpose (the API server Service),
so with these rules applied `DescryTargetDown` fires for it after 5m and stays
firing: remove that target first, or expect the alert.

A program that embeds the `metrics` package gets these alerts only for what it
wires up: `DescryTargetDown` and `DescryChecksStale` need its check wrapped
with `Collector.Check`, `DescrySkipsClimbing` and `DescryResultsDropped` need
`SetRunner`, and `DescryPublishErrors` needs `Options.ResultErrors`. Without
that wiring the series still exist but stay at 0 (or are never written), so
the alert can never fire.

## The stale-check budget

`DescryChecksStale` catches a prober that is up and serving `/metrics` but no
longer completing checks. `descry_check_up` keeps its last value (a target
that was up stays up) and `descry_build_info` is still scraped. A wedged sink
keeps its runs in flight, so their slots are skipped and `DescrySkipsClimbing`
fires as well, but that alert points at the concurrency or the interval;
`DescryChecksStale` is the one that names the real symptom. A wedged scheduler
starts no runs and skips no slots, so only `DescryChecksStale` catches it.

descry does not export the check interval, so the budget is fixed at 15
minutes since a target's last completed run. Keep it above twice your longest
per-target interval plus the timeout, or a slow target fires the alert between
two healthy runs: edit the `15 * 60` in the rule (and the 15 minutes in its
text) if your intervals are longer. A target that has never completed a run
has no series and is not covered.

It reads the per-target series, so it needs `--metrics-per-target`, which is
on by default. With `--metrics-per-target=false` it never fires; alert on the
fleet-wide histogram instead, which catches only the whole prober stalling,
not one target:

```yaml
- alert: DescryChecksStale
  expr: increase(descry_check_duration_seconds_count[15m]) == 0
  # Without the wait, two flat samples at startup page before a slow first
  # run completes. With it, the alert fires about 30 minutes after the last
  # completed run: 15 to empty the window, 15 more to wait.
  for: 15m
  labels:
    severity: critical
```

The histogram records completed observations only. A check that returns an
error from `Run` is not recorded there, so in a program that embeds descry
with such a check, a prober whose every run errors looks stalled to this
rule. The CLI's HTTP check never returns one.

A program that embeds descry and stops checking a URL keeps that URL's series,
frozen at their last values, until it restarts, so `DescryChecksStale` fires
for it until then. The CLI checks a fixed set of targets for its whole
lifetime, so this does not arise there.

## Scrape interval

Scrape descry every minute or more often. `DescrySkipsClimbing`,
`DescryResultsDropped` and `DescryPublishErrors` use `increase(...[10m])`,
which needs two samples inside the window. At a 5-minute scrape interval one
missed scrape leaves a 10-minute gap, no window holds samples from both sides
of it, and a counter step inside the gap never fires the alert.

## What the alerts do not catch

- **One dead instance among several.** `DescryProberAbsent` uses `absent()`,
  which looks at every `descry_build_info` series in the Prometheus. If it
  scrapes more than one descry instance (replicas, namespaces, or clusters
  that send to one Prometheus), it fires only when all of them are gone. For
  an instance that is still discovered but cannot be scraped, add a rule on
  the scrape job's `up` series, with the job label your setup gives descry:

  ```yaml
  - alert: DescryInstanceDown
    expr: up{job="<your descry job>"} == 0
    for: 5m
    labels:
      severity: critical
  ```

  An instance that disappears from service discovery altogether (its pod
  deleted, or scaled to 0) takes its `up` series with it, so that rule does
  not see it either. Catch that with `absent(up{job="<your descry job>"})`
  (one rule per cluster or namespace, with that label in the selector, if one
  Prometheus scrapes several).

- **One failed run of a slow target is enough.** `descry_check_up` holds its
  value until the next run, so the 5m wait on `DescryTargetDown` is about 5m
  divided by the target's interval in consecutive failures. A target with an
  interval of 5m or more pages after a single failed run. Raise `for` if slow
  targets need more than one chance.
- **The skip alert is not "skipping for 15 minutes straight".** It fires when
  every 10-minute window over 15 minutes holds at least one skip: skipping that
  lasts about 8 minutes or more, or single skips less than 10 minutes apart.
- **Failures before a new instance's first scrape.** `increase()` needs two
  samples of a series, so a counter that is already above 0 when its series
  first appears (a new pod after a rollout or reschedule gets a new instance
  label) never fires `DescryResultsDropped` or `DescryPublishErrors`. Check the
  pod log after a restart. A restart that keeps the same series is handled as a
  counter reset.
- **A descry restart resolves `DescryTargetDown`.** `descry_check_up` carries
  the scrape's `instance` label (and usually `pod`, under a `PodMonitor` or
  annotation-based discovery), so after a rollout or reschedule the old
  series goes stale, the alert resolves, and a target that is still down
  pages again only once the new pod has checked it and the 5m wait has
  passed. If that must not happen, do both: aggregate those labels away, for
  example `min without (instance, pod) (descry_check_up) == 0` (which keeps
  `job`, `url` and any cluster label apart, where `min by (url)` would merge
  the same URL from different clusters or jobs), and set `keep_firing_for` on
  the rule to more than the restart gap plus the target's interval. Either one
  alone still lets the alert resolve: the aggregate has no series for the url
  until the new pod has checked it, and without the aggregate the new pod's
  series is a new alert that waits out the 5m again.

## With the Prometheus Operator

```sh
kubectl apply -f contrib/prometheus/descry-rules.yaml
```

The Operator only picks up a rule whose labels match its `ruleSelector`. If
yours selects on a label such as `release`, add it under `metadata.labels`
(and set `metadata.namespace` if the rule must live in a particular
namespace).

The Operator ignores `prometheus.io/*` pod annotations, so it also needs a
`PodMonitor` (or a `ServiceMonitor` over a Service) for the descry pod.
Without one nothing scrapes descry and `DescryProberAbsent` fires from the
start. For the Kubernetes example, which names its container port `metrics`:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: descry
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: descry
  podMetricsEndpoints:
    - port: metrics
      path: /metrics
```

Create it in the descry pod's namespace (one the Operator watches), and give it
the labels your Operator's `podMonitorSelector` expects, as with the rule.

Use one discovery style, not both. The Kubernetes example's pod also carries
`prometheus.io/*` annotations, and where a Prometheus has an annotation-based
pod scrape job as well as the Operator's `PodMonitor` jobs (an extra scrape
config, say), it scrapes the pod twice. Every series then exists twice, under
two `job` labels, and every alert but `DescryProberAbsent` fires twice. With a `PodMonitor`,
remove the annotations from the Deployment (or make sure no annotation-based
job covers that namespace).

## With plain Prometheus

Extract the rule groups with [yq](https://github.com/mikefarah/yq) (v4) and
list the result under `rule_files`:

```sh
yq '.spec' contrib/prometheus/descry-rules.yaml > /etc/prometheus/rules/descry-rules.yaml
```

## Tests

`descry-rules.test.yaml` holds [promtool unit tests](https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/)
for every alert. Run them from this directory:

```sh
yq '.spec' descry-rules.yaml > descry-rules.extracted.yaml   # git-ignored
promtool check rules --lint-fatal descry-rules.extracted.yaml
promtool test rules descry-rules.test.yaml
```

The CI `rules` job runs exactly these steps with pinned `yq` and `promtool`
binaries. Because a rule over a series that no longer exists simply never
fires, `go test ./metrics/` also reads `descry-rules.yaml` and fails if any
series it names is not registered by the collector, or if a label it reads
(`{{ $labels.url }}`, a `by (...)` list, a selector such as `{kind}`) is not
on that series, so a renamed metric or label breaks the build instead of
silently disabling an alert or blanking its text. When an alert's text
changes, update the matching annotations in the test file.
