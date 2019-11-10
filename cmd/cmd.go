// Package cmd groups supported zookeeper client console commands
package cmd

import (
	"flag"
	"fmt"

	"github.com/medusar/zooklient/util"
)

//SupportedCmds stores all the supported commands for zookeeper
var SupportedCmds = []ZkCmd{
	&LsCmd{}, &GetCmd{}, &StatCmd{}, &SetCmd{},
	&DeleteCmd{}, &CreateCmd{}, &DeleteAllCmd{},
}

//ZkCmd represents a console command of zk client
type ZkCmd interface {
	//Usage get the usage information of the cmd
	Usage() string
}

// LsCmd stores all the parameters followed by `ls`,
// usage: ls [-s] [-w] [-R] path
type LsCmd struct {
	WithStat  bool
	Watch     bool
	Recursive bool
	Path      string
}

func (*LsCmd) Usage() string {
	return "ls [-s] [-w] [-R] path"
}

// ParseLs parses console command line into LsCmd
func ParseLs(args []string) (*LsCmd, error) {
	cmd := flag.NewFlagSet("ls", flag.ContinueOnError)
	withStat := cmd.Bool("s", false, "with stat")
	watch := cmd.Bool("w", false, "watch")
	recursive := cmd.Bool("R", false, "recursive")
	err := cmd.Parse(args)
	if err != nil {
		return nil, err
	}
	path := cmd.Arg(0)
	if path == "" {
		return nil, fmt.Errorf("ls [-s] [-w] [-R] path")
	}
	return &LsCmd{WithStat: *withStat, Watch: *watch, Recursive: *recursive, Path: path}, nil
}

// GetCmd stores all the parameters followed by `get`,
// usage: get [-s] [-w] path
type GetCmd struct {
	Watch    bool
	WithStat bool
	Path     string
}

func (*GetCmd) Usage() string {
	return "get [-s] [-w] path"
}

// ParseGet parses console command line into GetCmd
func ParseGet(args []string) (*GetCmd, error) {
	cmdFlag := flag.NewFlagSet("get", flag.ContinueOnError)
	withStat := cmdFlag.Bool("s", false, "with stat")
	watch := cmdFlag.Bool("w", false, "watch")
	err := cmdFlag.Parse(args)
	if err != nil {
		return nil, err
	}
	path := cmdFlag.Arg(0)
	if path == "" {
		return nil, fmt.Errorf("get [-s] [-w] path")
	}
	return &GetCmd{*watch, *withStat, path}, nil
}

// StatCmd stores all the parameters followed by `stat`,
type StatCmd struct {
	Watch bool
	Path  string
}

func (*StatCmd) Usage() string {
	return "stat [-w] path"
}

func ParseStat(args []string) (*StatCmd, error) {
	cmd := flag.NewFlagSet("stat", flag.ContinueOnError)
	watch := cmd.Bool("w", false, "with watch")
	err := cmd.Parse(args)
	if err != nil {
		return nil, err
	}
	path := cmd.Arg(0)
	if path == "" {
		return nil, fmt.Errorf("stat [-w] path")
	}
	return &StatCmd{*watch, path}, nil
}

// SetCmd stores all the parameters followed by `set`,
// usage: set [-s] [-v version] path data
type SetCmd struct {
	WithStat bool
	Version  int32
	Path     string
	Data     string
}

func (*SetCmd) Usage() string {
	return "set [-s] [-v version] path data"
}

func ParseSet(args []string) (*SetCmd, error) {
	cmd := flag.NewFlagSet("set", flag.ContinueOnError)
	withStat := cmd.Bool("s", false, "with stat")
	version := cmd.Int64("v", -1, "version")
	if err := cmd.Parse(args); err != nil {
		return nil, err
	}
	if cmd.NArg() != 2 {
		return nil, fmt.Errorf("set [-s] [-v version] path data")
	}
	path := cmd.Arg(0)
	if path == "" {
		return nil, fmt.Errorf("set [-s] [-v version] path data")
	}
	data := cmd.Arg(1)
	if data == "" {
		return nil, fmt.Errorf("set [-s] [-v version] path data")
	}
	return &SetCmd{*withStat, int32(*version), path, data}, nil
}

//DeleteCmd stores all the parameters followed by `delete`,
//usage: delete [-v version] path
type DeleteCmd struct {
	Version int32
	Path    string
}

func (*DeleteCmd) Usage() string {
	return "delete [-v version] path"
}

func ParseDelete(args []string) (*DeleteCmd, error) {
	cmd := flag.NewFlagSet("delete", flag.ContinueOnError)
	version := cmd.Int64("v", -1, "version")
	if err := cmd.Parse(args); err != nil {
		return nil, err
	}
	path := cmd.Arg(0)
	if path == "" {
		return nil, fmt.Errorf("delete [-v version] path")
	}
	return &DeleteCmd{int32(*version), path}, nil
}

//CreateCmd stores all the parameters followed by `create`,
//usage: create [-s] [-e] [-c] [-t ttl] path [data] [acl]
type CreateCmd struct {
	HasS bool
	HasE bool
	HasC bool
	HasT bool
	TTL  int64
	Path string
	Data string
	ACL  string
}

func (*CreateCmd) Usage() string {
	return "create [-s] [-e] [-c] [-t ttl] path [data] [acl]"
}

func ParseCreate(args []string) (*CreateCmd, error) {
	cmd := flag.NewFlagSet("create", flag.ContinueOnError)
	hasS := cmd.Bool("s", false, "sequential")
	hasE := cmd.Bool("e", false, "ephemeral")
	hasC := cmd.Bool("c", false, "container")
	ttl := cmd.Int64("t", 0, "ttl")
	if err := cmd.Parse(args); err != nil {
		return nil, err
	}

	hasT := util.IsOptionSet("t", cmd)
	path := cmd.Arg(0)
	if path == "" {
		return nil, fmt.Errorf("create [-s] [-e] [-c] [-t ttl] path [data] [acl]")
	}
	data := cmd.Arg(1)
	acl := cmd.Arg(2)
	return &CreateCmd{HasS: *hasS, HasE: *hasE, HasC: *hasC, HasT: hasT, TTL: *ttl, Path: path, Data: data, ACL: acl}, nil
}

//DeleteAllCmd stores all the parameters followed by `deleteall`,
//usage: deleteall path
type DeleteAllCmd struct {
	Path string
}

func (*DeleteAllCmd) Usage() string {
	return "deleteall path"
}

func ParseDeleteAll(args []string) (*DeleteAllCmd, error) {
	cmd := flag.NewFlagSet("deleteall", flag.ContinueOnError)
	if err := cmd.Parse(args); err != nil {
		return nil, err
	}
	path := cmd.Arg(0)
	if path == "" {
		return nil, fmt.Errorf("deleteall path")
	}
	return &DeleteAllCmd{path}, nil
}
