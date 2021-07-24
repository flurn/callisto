package main

import (
	"github.com/flurn/callisto/app"
	"github.com/flurn/callisto/config"
	"github.com/flurn/callisto/server"
	"github.com/urfave/cli"
	"os"
)

func main() {
	clientApp := cli.NewApp()
	clientApp.Name = "Callisto App"
	clientApp.Version = "0.0.1"
	clientApp.Commands = []cli.Command{
		{
			Name:        "customer",
			Description: "Run service",
			Action: func(c *cli.Context) error {
				config.Load(config.AppServer)
				app.InitApp()
				defer app.StopApp()

				server.StartAPIServer()
				return nil
			},
		},
		{
			Name:        "consumer-worker",
			Description: "Run consumer as worker",
			Action: func(c *cli.Context) error {
				config.Load(config.ConsumerAsWorker)
				app.InitApp()
				defer app.StopApp()

				server.StartConsumerASWorker()
				return nil
			},
		},
	}

	if err := clientApp.Run(os.Args); err != nil {
		panic(err)
	}
}
