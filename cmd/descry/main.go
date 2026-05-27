package main

import (
	"context"
	"os"
	"time"

	"github.com/fairbearlab/descry/check"
	httpcheck "github.com/fairbearlab/descry/checks/http"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/sink"
)

func main() {
	ctx := context.Background()
	c := httpcheck.New(10 * time.Second)
	t := check.Target{URL: "https://example.com", Labels: map[string]string{"url": "https://example.com"}}
	obs, _ := c.Run(ctx, t)
	e, err := event.ToCloudEvent(obs, event.Config{Source: "descry/cli"})
	if err != nil {
		panic(err)
	}
	if err := sink.NewStdoutSink(os.Stdout).Publish(ctx, e); err != nil {
		panic(err)
	}
}
