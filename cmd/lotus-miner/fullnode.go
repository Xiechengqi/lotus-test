package main

import (
	"fmt"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"strconv"
	"strings"
)

var fullnodeCmd = &cli.Command{
	Name:  "fullnode",
	Usage: "Manage Full Node",
	Subcommands: []*cli.Command{
		listCmd,
		addCmd,
		removeCmd,
		currentCmd,
	},
}

var removeCmd = &cli.Command{
	Name:      "remove",
	Usage:     "remove full nodes",
	ArgsUsage: "<index>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 1 {
			return xerrors.Errorf("missing param")
		}
		index, err := strconv.Atoi(cctx.Args().First())
		if err != nil {
			return err
		}

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		err = nodeApi.FullNodeRemove(ctx, index)
		if err != nil {
			return err
		}
		fmt.Println("remove fullnode success.")
		return nil
	},
}

var addCmd = &cli.Command{
	Name:      "add",
	Usage:     "add full nodes",
	ArgsUsage: "<auth api-info result>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 1 {
			return xerrors.Errorf("missing param")
		}
		apiinfo := cctx.Args().First()
		fmt.Println("adding fullnode: ", apiinfo)

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		err = nodeApi.FullNodeAdd(ctx, apiinfo)
		if err != nil {
			return err
		}
		fmt.Println("add fullnode success.")
		return nil
	},
}

var currentCmd = &cli.Command{
	Name:      "current",
	Usage:     "set current",
	ArgsUsage: "<index>",
	Action: func(cctx *cli.Context) error {
		if cctx.Args().Len() < 1 {
			return xerrors.Errorf("missing param")
		}
		newCurrent, err := strconv.Atoi(cctx.Args().First())
		if err != nil {
			return err
		}
		fmt.Println("set current fullnode to", newCurrent)

		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		err = nodeApi.FullNodeSetCurrent(ctx, newCurrent)
		if err != nil {
			return err
		}
		fmt.Println("set fullnode current success.")
		return nil
	},
}

var listCmd = &cli.Command{
	Name:  "list",
	Usage: "list full nodes",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name: "verbose",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		list, err := nodeApi.FullNodeList(ctx)
		if err != nil {
			return err
		}
		vv := cctx.Bool("verbose")
		for i, item := range list {
			var id string
			if !vv {
				p := strings.SplitN(item.ID, ":", 2)
				if len(p) >= 2 {
					id = p[1]
				}
			} else {
				id = item.ID
			}
			if item.Err != "" {
				fmt.Printf("%d: %s error: %v", i, id, item.Err)
			} else {
				fmt.Printf("%d: %s latest: %v", i, id, item.Latest)
			}
			if item.Current {
				fmt.Print(" X")
			}
			fmt.Println()
		}
		return nil
	},
}
