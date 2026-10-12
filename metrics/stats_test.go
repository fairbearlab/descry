package metrics_test

import (
	"github.com/fairbearlab/descry/metrics"
	"github.com/fairbearlab/descry/runner"
)

// A *runner.Runner is the RunnerStats passed to (*Collector).SetRunner.
var _ metrics.RunnerStats = (*runner.Runner)(nil)
