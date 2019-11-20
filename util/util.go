package util

import (
	"flag"
	"fmt"
	"strings"

	"github.com/samuel/go-zookeeper/zk"
)

// ValidatePath checks wether a zookeeper path is valid.
// If path is not valid, an error is returned.
// If path is valid, nil is returned.
func ValidatePath(path string) error {
	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}
	if string(path[0]) != "/" {
		return fmt.Errorf("path must start with / character")
	}
	if len(path) == 1 { // done checking - it's the root
		return nil
	}

	if string(path[len(path)-1]) == "/" {
		return fmt.Errorf("path must not end with / character")
	}

	var reason string
	lastr := '/'
	chars := []rune(path)
	for i := 1; i < len(chars); i++ {
		r := chars[i]
		if r == 0 {
			reason = fmt.Sprintf("null character not allowed @%d", i)
			break
		} else if r == '/' && lastr == '/' {
			reason = fmt.Sprintf("empty node name specified @%d", i)
			break
		} else if r == '.' && lastr == '.' {
			if chars[i-2] == '/' && (i+1 == len(chars) || chars[i+1] == '/') {
				reason = fmt.Sprintf("relative paths not allowed @%d", i)
				break
			}
		} else if r == '.' {
			if chars[i-1] == '/' && (i+1 == len(chars) || chars[i+1] == '/') {
				reason = fmt.Sprintf("relative paths not allowed @%d", i)
				break
			}
		} else if r > '\u0000' && r <= '\u001f' || r >= '\u007f' && r <= '\u009F' || r >= 0xd800 && r <= 0xf8ff || r >= 0xfff0 && r <= 0xffff {
			reason = fmt.Sprintf("invalid character @%d", i)
			break
		}

		lastr = r
	}
	if reason != "" {
		return fmt.Errorf("Invalid path string \"" + path + "\" caused by " + reason)
	}
	return nil
}

//ParseACL parses acl strings into zk.ACL;
//notation of acl: `scheme:id:perm`
func ParseACL(acl string) []zk.ACL {
	acls := strings.Split(acl, ",")
	zkACLs := make([]zk.ACL, 0)
	for _, a := range acls {
		firstColon := strings.Index(a, ":")
		lastColon := strings.LastIndex(a, ":")
		if firstColon == -1 || lastColon == -1 || firstColon == lastColon {
			fmt.Println(a + " does not have the form scheme:id:perm")
			continue
		}
		perms := parsePerms(a[lastColon+1:])
		zkACLs = append(zkACLs, zk.ACL{Scheme: a[0:firstColon], ID: a[firstColon+1 : lastColon], Perms: perms})
	}
	return zkACLs
}

func parsePerms(s string) int32 {
	perm := int32(0)
	for _, r := range s {
		char := string(r)
		switch char {
		case "r":
			perm |= zk.PermRead
		case "w":
			perm |= zk.PermWrite
		case "c":
			perm |= zk.PermCreate
		case "d":
			perm |= zk.PermDelete
		case "a":
			perm |= zk.PermAdmin
		default:
			fmt.Println("Unknown perm type:", char)
		}
	}
	return perm
}

//IsOptionSet checks if an option is set in command line
func IsOptionSet(name string, f *flag.FlagSet) bool {
	set := false
	f.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}
