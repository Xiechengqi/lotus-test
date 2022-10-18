package cli

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var EnvCmd = &cli.Command{
	Name:  "env",
	Usage: "Check and set environment variables",
	Subcommands: []*cli.Command{
		SetEnv,
		GetEnv,
		GetAllEnv,
	},
}

var SetEnv = &cli.Command{
	Name:  "set-env",
	Usage: "Set environment variables",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 2 {
			return xerrors.Errorf("expected 2 argument")
		}

		napi, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		v := cctx.Args().First()
		vv := cctx.Args().Get(1)
		err = napi.SetEnv(ctx, v, vv)
		if err != nil {
			return err
		}
		fmt.Printf("set %s to %s succeed.\n", v, vv)

		return nil
	},
}

var GetEnv = &cli.Command{
	Name:  "get-env",
	Usage: "Get environment variables",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() != 1 {
			return xerrors.Errorf("expected 1 argument")
		}
		napi, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		val, err := napi.GetEnv(ctx, cctx.Args().First())
		if err != nil {
			return err
		}
		fmt.Println(val)
		return nil
	},
}

var GetAllEnv = &cli.Command{
	Name:  "list",
	Usage: "Print all environment variables",
	Action: func(cctx *cli.Context) error {
		napi, closer, err := GetAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		envs, err := napi.GetAllEnv(ctx)
		if err != nil {
			return err
		}
		for k, v := range envs {
			fmt.Printf("%v=%v\n", k, v)
		}
		return nil
	},
}
