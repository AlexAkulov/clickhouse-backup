package main

import (
	"fmt"
	"log"
	"os"

	"github.com/AlexAkulov/clickhouse-backup/pkg/chbackup"

	"github.com/urfave/cli"
)

const (
	defaultConfigPath = "/etc/clickhouse-backup/config.yml"
)

var (
	version   = "unknown"
	gitCommit = "unknown"
	buildDate = "unknown"
)

func main() {
	log.SetOutput(os.Stdout)
	cliapp := cli.NewApp()
	cliapp.Name = "clickhouse-backup"
	cliapp.Usage = "Tool for easy backup of ClickHouse with cloud support"
	cliapp.UsageText = "clickhouse-backup <command> [-t, --tables=<db>.<table>] <backup_name>"
	cliapp.Description = "Run as 'root' or 'clickhouse' user"
	cliapp.Version = version

	cliapp.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "config, c",
			Value:  defaultConfigPath,
			Usage:  "Config `FILE` name.",
			EnvVar: "CLICKHOUSE_BACKUP_CONFIG",
		},
	}
	cliapp.CommandNotFound = func(c *cli.Context, command string) {
		fmt.Printf("Error. Unknown command: '%s'\n\n", command)
		cli.ShowAppHelpAndExit(c, 1)
	}

	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Println("Version:\t", c.App.Version)
		fmt.Println("Git Commit:\t", gitCommit)
		fmt.Println("Build Date:\t", buildDate)
	}

	cliapp.Commands = []cli.Command{
		{
			Name:      "tables",
			Usage:     "Print list of tables",
			UsageText: "clickhouse-backup tables",
			Action: func(c *cli.Context) error {
				return chbackup.PrintTables(*getConfig(c))
			},
			Flags: cliapp.Flags,
		},
		{
			Name:        "create",
			Usage:       "Create new backup",
			UsageText:   "clickhouse-backup create [-t, --tables=<db>.<table>] <backup_name>",
			Description: "Create new backup",
			Action: func(c *cli.Context) error {
				return chbackup.CreateBackup(*getConfig(c), c.Args().First(), c.String("t"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
				},
			),
		},
		{
			Name:      "upload",
			Usage:     "Upload backup to remote storage",
			UsageText: "clickhouse-backup upload [--diff-from=<backup_name>] <backup_name>",
			Action: func(c *cli.Context) error {
				return chbackup.Upload(*getConfig(c), c.Args().First(), c.String("diff-from"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "diff-from",
					Hidden: false,
				},
			),
		},
		{
			Name:      "list",
			Usage:     "Print list of backups",
			UsageText: "clickhouse-backup list [all|local|remote] [latest|penult]",
			Action: func(c *cli.Context) error {
				config := getConfig(c)
				switch c.Args().Get(0) {
				case "local":
					return chbackup.PrintLocalBackups(*config, c.Args().Get(1))
				case "remote":
					return chbackup.PrintRemoteBackups(*config, c.Args().Get(1))
				case "all", "":
					fmt.Println("Local backups:")
					if err := chbackup.PrintLocalBackups(*config, c.Args().Get(1)); err != nil {
						return err
					}
					fmt.Println("Remote backups:")
					if err := chbackup.PrintRemoteBackups(*config, c.Args().Get(1)); err != nil {
						return err
					}
				default:
					fmt.Fprintf(os.Stderr, "Unknown command '%s'\n", c.Args().Get(0))
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				return nil
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "download",
			Usage:     "Download backup from remote storage",
			UsageText: "clickhouse-backup download <backup_name>",
			Action: func(c *cli.Context) error {
				return chbackup.Download(*getConfig(c), c.Args().First())
			},
			Flags: cliapp.Flags,
		},
		{
			Name:      "restore",
			Usage:     "Create schema and restore data from backup",
			UsageText: "clickhouse-backup restore [--schema] [--data] [-t, --tables=<db>.<table>] <backup_name>",
			Action: func(c *cli.Context) error {
				return chbackup.Restore(*getConfig(c), c.Args().First(), c.String("t"), c.Bool("s"), c.Bool("d"))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
				},
				cli.BoolFlag{
					Name:   "schema, s",
					Hidden: false,
					Usage:  "Restore schema only",
				},
				cli.BoolFlag{
					Name:   "data, d",
					Hidden: false,
					Usage:  "Restore data only",
				},
			),
		},
		{
			Name:      "delete",
			Usage:     "Delete specific backup",
			UsageText: "clickhouse-backup delete <local|remote> <backup_name>",
			Action: func(c *cli.Context) error {
				config := getConfig(c)
				if c.Args().Get(1) == "" {
					fmt.Fprintln(os.Stderr, "Backup name must be defined")
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				switch c.Args().Get(0) {
				case "local":
					return chbackup.RemoveBackupLocal(*config, c.Args().Get(1))
				case "remote":
					return chbackup.RemoveBackupRemote(*config, c.Args().Get(1))
				default:
					fmt.Fprintf(os.Stderr, "Unknown command '%s'\n", c.Args().Get(0))
					cli.ShowCommandHelpAndExit(c, c.Command.Name, 1)
				}
				return nil
			},
			Flags: cliapp.Flags,
		},
		{
			Name:  "default-config",
			Usage: "Print default config",
			Action: func(*cli.Context) {
				chbackup.PrintDefaultConfig()
			},
			Flags: cliapp.Flags,
		},
		{
			Name:        "freeze",
			Usage:       "Freeze tables",
			UsageText:   "clickhouse-backup freeze [-t, --tables=<db>.<table>] <backup_name>",
			Description: "Freeze tables",
			Action: func(c *cli.Context) error {
				return chbackup.Freeze(*getConfig(c), c.String("t"), c.Args().Get(1))
			},
			Flags: append(cliapp.Flags,
				cli.StringFlag{
					Name:   "table, tables, t",
					Hidden: false,
				},
			),
		},
		{
			Name:  "clean",
			Usage: "Remove data in 'shadow' folder",
			Action: func(c *cli.Context) error {
				return chbackup.Clean(*getConfig(c))
			},
			Flags: cliapp.Flags,
		},
	}
	if err := cliapp.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func getConfig(ctx *cli.Context) *chbackup.Config {
	configPath := ctx.String("config")
	if configPath == defaultConfigPath {
		configPath = ctx.GlobalString("config")
	}

	config, err := chbackup.LoadConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}

	return config
}
