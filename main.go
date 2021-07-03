package main

import (
	"callisto/app"
	"callisto/config"
	"callisto/server"
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
	}

	if err := clientApp.Run(os.Args); err != nil {
		panic(err)
	}
}
