package metrics

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"

	"github.com/fairbearlab/descry/check"
)

const rulesPath = "../contrib/prometheus/descry-rules.yaml"

var (
	// The full metric-name character set, so a name with a digit or a colon
	// in it is read whole instead of being cut at the first such character.
	seriesName = regexp.MustCompile(`descry_[a-zA-Z0-9_:]+`)
	descFQName = regexp.MustCompile(`fqName: "([^"]+)"`)
	// Desc.String ends with the const and variable label lists.
	descLabels = regexp.MustCompile(`constLabels: \{(.*)\}, variableLabels: \{([^}]*)\}\}$`)
	// A selector or a label list after a series name, in an expr or in a
	// comment: descry_x{url="..."} or descry_x{kind}.
	seriesSelector = regexp.MustCompile(`(descry_[a-zA-Z0-9_:]+)\{([^}]*)\}`)
	selectorLabel  = regexp.MustCompile(`(?:^|,)\s*([a-zA-Z_][a-zA-Z0-9_]*)`)
	quotedValue    = regexp.MustCompile(`"(?:[^"\\]|\\.)*"`) // so a comma in a value is not a label
	templateLabel  = regexp.MustCompile(`\$labels\.([a-zA-Z_][a-zA-Z0-9_]*)`)
	groupingLabels = regexp.MustCompile(`\b(?:by|without|on|ignoring)\s*\(([^)]*)\)`)
)

// scrapeLabels are added by Prometheus at scrape time, not by the collector.
var scrapeLabels = map[string]bool{"instance": true, "job": true}

// TestAlertRulesReferenceRegisteredSeries fails when a series named in the
// shipped alerting rules is renamed or removed from the collector, which
// promtool cannot see: a rule over a missing series just never fires.
func TestAlertRulesReferenceRegisteredSeries(t *testing.T) {
	rules, err := os.ReadFile(rulesPath)
	if err != nil {
		t.Fatalf("read rules: %v", err)
	}
	c := newTestCollector(t, Options{}, fakeNow) // per-target series on
	registered := describeLabels(t, c)

	// Gather for the types, after one run so the per-target families exist.
	f := &fakeCheck{obs: check.Observation{Status: check.StatusUp, StatusCode: 200, TLSExpiry: &expiry}}
	if _, err := c.Check(f).Run(context.Background(), check.Target{URL: "https://h/"}); err != nil {
		t.Fatal(err)
	}
	c.ObserveSkip(SkipQueued)
	mfs, err := c.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	// The type by name (HISTOGRAM, COUNTER, ...), read through String so this
	// test does not import client_model, which go.mod lists as indirect.
	kind := make(map[string]string)
	for _, mf := range mfs {
		if registered[mf.GetName()] == nil {
			t.Errorf("Gather reports %s, which Describe does not", mf.GetName())
		}
		kind[mf.GetName()] = mf.GetType().String()
	}

	used := make(map[string]bool)
	for _, name := range seriesName.FindAllString(string(rules), -1) {
		used[name] = true
		if registered[name] != nil {
			continue
		}
		if !isSampleOf(name, registered, kind) {
			t.Errorf("rules reference %s, which the collector does not register", name)
		}
	}
	if len(used) < 5 {
		t.Errorf("found %d distinct series names in the rules, want at least 5", len(used))
	}
}

// TestAlertRulesReferenceRegisteredLabels fails when a label the rules read
// is not on the series they read it from: a {{ $labels.x }} in an annotation,
// a by/without/on/ignoring list in an expr, or a selector such as
// descry_x{kind} anywhere in the file, comments included. A renamed label
// otherwise leaves the rule firing with an empty value in its text, which
// neither promtool (its tests write their own labels) nor the series-name
// test above can see.
func TestAlertRulesReferenceRegisteredLabels(t *testing.T) {
	raw, err := os.ReadFile(rulesPath)
	if err != nil {
		t.Fatalf("read rules: %v", err)
	}
	labels := describeLabels(t, newTestCollector(t, Options{}, fakeNow))
	checked := 0
	has := func(series []string, label string) bool {
		if scrapeLabels[label] {
			return true
		}
		for _, s := range series {
			if labelsOf(s, labels)[label] {
				return true
			}
		}
		return false
	}

	for _, m := range seriesSelector.FindAllStringSubmatch(string(raw), -1) {
		for _, l := range selectorLabel.FindAllStringSubmatch(quotedValue.ReplaceAllString(m[2], `""`), -1) {
			checked++
			if !has([]string{m[1]}, l[1]) {
				t.Errorf("rules use %s{%s}, but %s has no label %q", m[1], m[2], m[1], l[1])
			}
		}
	}

	var pr struct {
		Spec struct {
			Groups []struct {
				Rules []struct {
					Alert       string            `yaml:"alert"`
					Expr        string            `yaml:"expr"`
					Labels      map[string]string `yaml:"labels"`
					Annotations map[string]string `yaml:"annotations"`
				} `yaml:"rules"`
			} `yaml:"groups"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(raw, &pr); err != nil {
		t.Fatalf("parse rules: %v", err)
	}
	for _, g := range pr.Spec.Groups {
		for _, r := range g.Rules {
			series := seriesName.FindAllString(r.Expr, -1)
			var refs []string
			for _, text := range []map[string]string{r.Labels, r.Annotations} {
				for _, v := range text {
					for _, m := range templateLabel.FindAllStringSubmatch(v, -1) {
						refs = append(refs, m[1])
					}
				}
			}
			for _, m := range groupingLabels.FindAllStringSubmatch(r.Expr, -1) {
				for _, l := range strings.Split(m[1], ",") {
					if l = strings.TrimSpace(l); l != "" {
						refs = append(refs, l)
					}
				}
			}
			for _, l := range refs {
				checked++
				if !has(series, l) {
					t.Errorf("%s reads label %q, which none of %q has", r.Alert, l, series)
				}
			}
		}
	}
	if checked < 2 {
		t.Errorf("checked %d label references in the rules, want at least 2", checked)
	}
}

// describeLabels maps every series the collector describes to its label
// names, const and variable. Describe, not Gather: Gather omits a vec with no
// children yet.
func describeLabels(t *testing.T, c *Collector) map[string]map[string]bool {
	t.Helper()
	descs := make(chan *prometheus.Desc, 64)
	go func() { c.Registry().Describe(descs); close(descs) }()
	out := make(map[string]map[string]bool)
	for d := range descs {
		s := d.String()
		m := descFQName.FindStringSubmatch(s)
		if m == nil {
			continue
		}
		set := make(map[string]bool)
		if l := descLabels.FindStringSubmatch(s); l != nil {
			for _, p := range strings.Split(l[1], ",") {
				if name, _, ok := strings.Cut(p, "="); ok {
					set[name] = true
				}
			}
			for _, name := range strings.Split(l[2], ",") {
				if name != "" {
					set[strings.TrimSuffix(strings.TrimPrefix(name, "c("), ")")] = true
				}
			}
		} else {
			t.Errorf("cannot read the labels of %s", s)
		}
		out[m[1]] = set
	}
	return out
}

// labelsOf returns the labels of a series name, resolving a histogram's
// _bucket (which adds le), _sum or _count to its family.
func labelsOf(name string, labels map[string]map[string]bool) map[string]bool {
	if set, ok := labels[name]; ok {
		return set
	}
	for _, suf := range []string{"_bucket", "_sum", "_count"} {
		if base, ok := strings.CutSuffix(name, suf); ok && labels[base] != nil {
			if suf != "_bucket" {
				return labels[base]
			}
			set := map[string]bool{"le": true}
			for l := range labels[base] {
				set[l] = true
			}
			return set
		}
	}
	return nil
}

// isSampleOf reports whether name is one of the per-sample series a histogram
// or summary family exposes: name_bucket (histograms only), name_sum or
// name_count. A gauge or counter has no such series.
func isSampleOf(name string, registered map[string]map[string]bool, kind map[string]string) bool {
	for _, suf := range []string{"_bucket", "_sum", "_count"} {
		base, ok := strings.CutSuffix(name, suf)
		if !ok || registered[base] == nil {
			continue
		}
		switch kind[base] {
		case "HISTOGRAM", "GAUGE_HISTOGRAM":
			return true
		case "SUMMARY":
			return suf != "_bucket"
		}
	}
	return false
}
