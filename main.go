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
)

var (
	zkconn = &zkConn{nil}
	//store history commands
	history = make([]string, 0)
	//default acls when creating nodes in zk
	unSafeACL = []zk.ACL{
		zk.ACL{Perms: zk.PermAll, Scheme: "world", ID: "anyone"},
	}

	server     = flag.String("server", "127.0.0.1:2181", "zooklient -server host:port cmd args")
	lastServer = ""
)

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

func isConnected(con *zk.Conn) bool {
	return con.State() == zk.StateHasSession
}

func loop() {
	reader := bufio.NewReader(os.Stdin)
	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading string", err)
			continue
		}
		command = strings.Replace(command, "\n", "", -1)
		err = processCommand(zkconn.con, command)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func processCommand(con *zk.Conn, command string) error {
	if command == "" {
		return nil
	}
	cmds := strings.Fields(command)
	if len(cmds) <= 0 {
		return nil
	}

	addToHistory(command)
	cmd := cmds[0]
	args := cmds[1:]

	switch cmd {
	case "history":
		count := len(history)
		for i := count - 10; i < count; i++ {
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
		break
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
		break
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
		break
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
		break
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
		break
	case "getAcl":
		fmt.Println("Not supported yet")
		break
	case "setAcl":
		fmt.Println("Not supported yet")
		break
	case "setquota":
	case "listquota":
	case "delquota":
	case "addauth":
	case "config":
		fmt.Println("Not supported yet")
		break
	case "sync":
		if len(args) != 1 {
			return fmt.Errorf("sync path")
		}
		_, err := con.Sync(args[0])
		if err != nil {
			return err
		}
		fmt.Println("Sync is OK")
		break
	case "delete":
		del, err := cmdParser.ParseDelete(args)
		if err != nil {
			return err
		}
		if err := con.Delete(del.Path, del.Version); err != nil {
			return err
		}
		break
	case "deleteall":
		fmt.Println("Not supported yet")
		break
	case "close":
		con.Close()
		break
	default:
		printUsage()
		fmt.Println("Command not found: Command not found", cmd)
		break
	}
	return nil
}

func addToHistory(cmd string) {
	history = append(history, cmd)
}

//TODO: print only supported cmds
func printUsage() {
	fmt.Println(`ZooKeeper -server host:port cmd args
		addauth scheme auth
		close
		config [-c] [-w] [-s]
		connect host:port
		create [-s] [-e] [-c] [-t ttl] path [data] [acl]
		delete [-v version] path
		deleteall path
		delquota [-n|-b] path
		get [-s] [-w] path
		getAcl [-s] path
		history
		listquota path
		ls [-s] [-w] [-R] path
		ls2 path [watch]
		printwatches on|off
		quit
		reconfig [-s] [-v version] [[-file path] | [-members serverID=host:port1:port2;port3[,...]*]] | [-add serverId=host:port1:port2;port3[,...]]* [-remove serverId[,...]*]
		redo cmdno
		removewatches path [-c|-d|-a] [-l]
		rmr path
		set [-s] [-v version] path data
		setAcl [-s] [-v version] [-R] path acl
		setquota -n|-b val path
		stat [-w] path
		sync path`)
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
