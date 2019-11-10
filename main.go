package main

import (
	"bufio"
	"context"
	"flag"
	"log"
	"os"
	"sort"

	"fmt"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	cmdParser "medusar.org/zooklient/cmd"
	"medusar.org/zooklient/util"
)

type zkConn struct {
	con *zk.Conn
}

const (
	extendMask       = uint64(0xff00000000000000)
	reservedBitsMask = uint64(0x00ffff0000000000)
	maxTTL           = ^(extendMask | reservedBitsMask)
	historyCount     = 10
)

var (
	zkconn = &zkConn{nil}
	//store history commands
	history = make([]string, 0, historyCount)
	//default acls when creating nodes in zk
	unSafeACL = []zk.ACL{
		zk.ACL{Perms: zk.PermAll, Scheme: "world", ID: "anyone"},
	}

	server     = flag.String("server", "127.0.0.1:2181", "zooklient -server host:port cmd args")
	lastServer = ""
)

func main() {
	flag.Parse()
	connectZK(*server)

	zkCmds := flag.Args()
	if zkCmds == nil || len(zkCmds) == 0 {
		loop()
	} else {
		// one time command line
		err := execZkCmd(zkconn.con, zkCmds[0], zkCmds[1:])
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		} else {
			os.Exit(0)
		}
	}
}

func connectZK(address string) {
	if zkconn.con != nil && zkconn.con.State() == zk.StateHasSession {
		//close existed connection first
		zkconn.con.Close()
	}

	con, conEvent, err := zk.Connect([]string{address}, 10*time.Second)
	if err != nil {
		fmt.Println(err)
		return
	}
	zkconn.con = con

	connected := make(chan struct{})
	go func() {
		for {
			select {
			case e := <-conEvent:
				if e.State == zk.StateHasSession {
					close(connected)
					return
				}
			}
		}
	}()

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	select {
	case <-connected:
		fmt.Println("zookeeper connected")
		//store last connected server
		lastServer = address
		break
	case <-ctx.Done():
		fmt.Println("Failed to connect zk server, please try later")
		con.Close()
		break
	}
}

func isConnected(con *zk.Conn) bool {
	return con.State() == zk.StateHasSession
}

func loop() {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading string", err)
			continue
		}
		line = strings.Replace(line, "\n", "", -1)
		err = execCmd(zkconn.con, line)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func execCmd(con *zk.Conn, line string) error {
	if line == "" {
		return nil
	}
	cmds := strings.Fields(line)
	if len(cmds) <= 0 {
		return nil
	}

	defer saveHistory(line)

	cmd := cmds[0]
	args := cmds[1:]

	switch cmd {
	case "history":
		count := len(history)
		for i := count - historyCount; i < count; i++ {
			if i < 0 {
				continue
			}
			fmt.Printf("%d - %s\n", i, history[i])
		}
		return nil
	case "quit":
		os.Exit(0)
		return nil
	case "connect":
		if len(args) == 0 {
			connectZK(lastServer)
		} else {
			connectZK(args[0])
		}
		return nil
	}

	if !isConnected(zkconn.con) {
		return fmt.Errorf("Not connected, use `connect` command to connect to a server")
	}

	return execZkCmd(con, cmd, args)
}

func execZkCmd(con *zk.Conn, cmd string, args []string) error {
	switch cmd {
	case "ls":
		ls, err := cmdParser.ParseLs(args)
		if err != nil {
			return err
		}
		if ls.Recursive {
			visitSubTree(con, ls.Path)
		} else {
			children, stat, err := con.Children(ls.Path)
			if err != nil {
				return err
			}
			if !ls.WithStat {
				printChildren(children, nil)
			} else {
				printChildren(children, stat)
			}
		}
	case "get":
		get, err := cmdParser.ParseGet(args)
		if err != nil {
			return err
		}
		data, stat, err := con.Get(get.Path)
		if err != nil {
			return err
		}
		if data == nil {
			fmt.Println("null")
		} else {
			fmt.Println(string(data))
		}
		if get.WithStat {
			printStat(stat)
		}
	case "stat":
		statCmd, err := cmdParser.ParseStat(args)
		if err != nil {
			return err
		}
		exists, stat, err := con.Exists(statCmd.Path)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("Node does not exist: %s", statCmd.Path)
		}
		printStat(stat)
	case "set":
		set, err := cmdParser.ParseSet(args)
		if err != nil {
			return err
		}
		stat, err := con.Set(set.Path, []byte(set.Data), set.Version)
		if err != nil {
			return err
		}
		if set.WithStat {
			printStat(stat)
		}
	case "create":
		create, err := cmdParser.ParseCreate(args)
		if err != nil {
			return err
		}
		if create.HasC && (create.HasE || create.HasS) {
			return fmt.Errorf("-c cannot be combined with -s or -e. containers cannot be ephemeral or sequential")
		}
		//TODO: validate ttl

		if create.HasT && create.HasE {
			return fmt.Errorf("TTLs cannot be used with Ephemeral znodes")
		}
		if create.HasT && create.HasC {
			return fmt.Errorf("TTLs cannot be used with Container znodes")
		}

		var flags int32
		if create.HasE && create.HasS {
			flags = 3 //CreateMode.EPHEMERAL_SEQUENTIAL
		} else if create.HasE {
			flags = zk.FlagEphemeral //CreateMode.EPHEMERAL
		} else if create.HasS {
			if create.HasT {
				flags = 6 //CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL
			} else {
				flags = zk.FlagSequence //CreateMode.PERSISTENT_SEQUENTIAL
			}
		} else if create.HasC {
			flags = 4 //CreateMode.CONTAINER
		} else {
			if create.HasT {
				flags = 5 //CreateMode.PERSISTENT_WITH_TTL
			} else {
				flags = 0 //CreateMode.PERSISTENT
			}
		}

		if create.HasT {
			if uint64(create.TTL) > maxTTL || create.TTL <= 0 {
				return fmt.Errorf("ttl must be positive and cannot be larger than: %d", maxTTL)
			}
		}

		acls := unSafeACL
		if create.ACL != "" {
			acls = util.ParseACL(create.ACL)
		}
		//TODO:java ttl support
		s, err := con.Create(create.Path, []byte(create.Data), flags, acls)
		if err != nil {
			return err
		}
		fmt.Println("Created", s)
	case "getAcl":
		fallthrough
	case "setAcl":
		fallthrough
	case "setquota":
		fallthrough
	case "listquota":
		fallthrough
	case "delquota":
		fallthrough
	case "addauth":
		fallthrough
	case "config":
		fmt.Println("Not supported yet")
	case "sync":
		if len(args) != 1 {
			return fmt.Errorf("sync path")
		}
		_, err := con.Sync(args[0])
		if err != nil {
			return err
		}
		fmt.Println("Sync is OK")
	case "delete":
		del, err := cmdParser.ParseDelete(args)
		if err != nil {
			return err
		}
		if err := con.Delete(del.Path, del.Version); err != nil {
			return err
		}
	case "deleteall":
		da, err := cmdParser.ParseDeleteAll(args)
		if err != nil {
			return err
		}
		return deleteRecursive(zkconn.con, da.Path)
	case "close":
		con.Close()
	default:
		printUsage()
		fmt.Println("Command not found: Command not found", cmd)
	}
	return nil
}

func saveHistory(cmd string) {
	history = append(history, cmd)
	if len(history) > historyCount {
		history = history[1 : historyCount+1]
	}
}

func printUsage() {
	fmt.Println("ZooKeeper -server host:port cmd args")
	fmt.Println("\t\t connect host:port")
	fmt.Println("\t\t history")
	fmt.Println("\t\t quit")
	for _, c := range cmdParser.SupportedCmds {
		fmt.Println("\t\t", c.Usage())
	}
}

func deleteRecursive(con *zk.Conn, path string) error {
	err := util.ValidatePath(path)
	if err != nil {
		return err
	}

	paths := listSubTreeBFS(con, path)

	deleteReqs := make([]interface{}, 0)
	total := len(paths)
	for i := total - 1; i >= 0; i-- {
		p := paths[i]
		deleteReqs = append(deleteReqs, &zk.DeleteRequest{Path: p, Version: -1})
	}

	if _, err := con.Multi(deleteReqs...); err != nil {
		return err
	}
	return nil
}

func listSubTreeBFS(con *zk.Conn, pathRoot string) []string {
	queue := make([]string, 0)
	tree := make([]string, 0)

	queue = append(queue, pathRoot)
	tree = append(tree, pathRoot)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		children, _, err := con.Children(node)
		if err != nil {
			log.Println(err)
			continue
		}
		for _, child := range children {
			childPath := node + "/" + child
			queue = append(queue, childPath)
			tree = append(tree, childPath)
		}
	}

	return tree
}

func visitSubTree(con *zk.Conn, path string) {
	err := util.ValidatePath(path)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(path)
	visitSubTreeBelow(con, path)
}

func visitSubTreeBelow(con *zk.Conn, path string) {
	root := path == "/"

	children, _, err := con.Children(path)
	if err != nil {
		log.Fatal(err)
	}
	sort.Strings(children)

	//print
	for _, child := range children {
		if root {
			fmt.Println(path + child)
		} else {
			fmt.Println(path + "/" + child)
		}
	}
	//iterate children
	for _, child := range children {
		if root {
			visitSubTreeBelow(con, path+child)
		} else {
			visitSubTreeBelow(con, path+"/"+child)
		}
	}
}

func printChildren(c []string, s *zk.Stat) {
	sort.Strings(c)
	fmt.Println("[" + strings.Join(c, ", ") + "]")
	if s != nil {
		printStat(s)
	}
}

func printStat(stat *zk.Stat) {
	fmt.Println("cZxid = 0x" + fmt.Sprintf("%x", stat.Czxid))
	ctime := time.Unix(0, stat.Ctime*int64(time.Millisecond))
	fmt.Println("ctime = " + ctime.String())
	fmt.Println("mZxid = 0x" + fmt.Sprintf("%x", stat.Mzxid))
	mtime := time.Unix(0, stat.Mtime*int64(time.Millisecond))
	fmt.Println("mtime = " + mtime.String())
	fmt.Println("pZxid = 0x" + fmt.Sprintf("%x", stat.Pzxid))
	fmt.Println("cversion =", stat.Cversion)
	fmt.Println("dataVersion =", stat.Version)
	fmt.Println("aclVersion =", stat.Aversion)
	fmt.Println("ephemeralOwner = 0x" + fmt.Sprintf("%x", stat.EphemeralOwner))
	fmt.Println("dataLength =", stat.DataLength)
	fmt.Println("numChildren =", stat.NumChildren)
}
