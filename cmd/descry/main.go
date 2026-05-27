package main

import (
	"context"
	"os"
	"time"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/sink"
)

func main() {
	obs := check.Observation{
		Status:     check.StatusUp,
		StatusCode: 200,
		LatencyMs:  42,
		ObservedAt: time.Now().UTC(),
		Labels:     map[string]string{"url": "https://example.com"},
	}
	e, err := event.ToCloudEvent(obs, event.Config{Source: "descry/stub"})
	if err != nil {
		panic(err)
	}
	if err := sink.NewStdoutSink(os.Stdout).Publish(context.Background(), e); err != nil {
		panic(err)
	}
}
