package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/govinda-attal/raft-server/server"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/urfave/cli/v2"
	"golang.org/x/exp/slog"
)

func main() {
	var (
		cfgFile string
		cfg     server.Config
		goCtx   = context.Background()
	)

	app := &cli.App{
		Name:  "server",
		Usage: "server runs demo raft protocol",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "config",
				Value:       "config.yaml",
				Usage:       "application config file",
				Aliases:     []string{"c"},
				Destination: &cfgFile,
			},
		},
		Before: func(ctx *cli.Context) (err error) {
			cfg, err = loadConfig(cfgFile)
			if err != nil {
				return err
			}
			setupLogger()
			return nil
		},
		Action: func(ctx *cli.Context) error {
			slog.Info("server running with configuration", "cfg", cfg)
			srv := server.New(cfg)

			done := make(chan os.Signal, 1)
			signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

			go func() {
				if err := srv.Start(goCtx); err != nil && err != http.ErrServerClosed {
					slog.Error("server stopped", "error", err)
				}
			}()
			log.Print("Server Started")

			<-done

			goCtx, cancel := context.WithTimeout(goCtx, 5*time.Second)
			defer func() {
				cancel()
			}()

			if err := srv.Shutdown(goCtx); err != nil {
				log.Fatalf("Server shutdown failed:%+v", err)
			}
			log.Print("Server shutdown gracefully")
			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func loadConfig(cfgFile string) (cfg server.Config, err error) {
	err = cleanenv.ReadConfig(cfgFile, &cfg)
	if err != nil {
		return
	}
	return cfg, nil
}

func setupLogger() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
}
