# zooklient
## A zookeeper command line client written in go.   
Sometimes when you want to check the contents in a zookeeper server, you need a command line zookeeper client.   
Although zookeeper provides a `zkCli.sh` in its release package, it is sometimes big and you will need to download and unzip and 
do some cds...   
With `zooklient`, all you need is to download and unzip, and execute the executable file. It is small in size and easy to use.   

## How to use

- Download the lastest release from [release page](https://github.com/medusar/zooklient/releases)
- unzip the zipfile
- execute ..

> You can also use go get to install on your computer if you have golang installed on you machine:

```
go get github.com/medusar/zooklient
```


## Usage Example
```
➜  zooklient git:(master) ✗ ./zooklient -server 127.0.0.1:2181
2019/11/10 15:24:20 Connected to 127.0.0.1:2181
2019/11/10 15:24:20 authenticated: id=72066667419402355, timeout=10000
zookeeper connected
2019/11/10 15:24:20 re-submitting `0` credentials after reconnect
ls /
[zookeeper]


ls -R /
/
/zookeeper
/zookeeper/config
/zookeeper/quota

create /home haha
Created /home

get /home
haha

stat /home
cZxid = 0x117
ctime = 2019-11-10 15:24:34.18 +0800 CST
mZxid = 0x117
mtime = 2019-11-10 15:24:34.18 +0800 CST
pZxid = 0x117
cversion = 0
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 4
numChildren = 0

ls -s /home
[]
cZxid = 0x117
ctime = 2019-11-10 15:24:34.18 +0800 CST
mZxid = 0x117
mtime = 2019-11-10 15:24:34.18 +0800 CST
pZxid = 0x117
cversion = 0
dataVersion = 0
aclVersion = 0
ephemeralOwner = 0x0
dataLength = 4
numChildren = 0


close
2019/11/10 15:25:26 recv loop terminated: err=EOF
2019/11/10 15:25:26 send loop terminated: err=<nil>

connect
2019/11/10 15:25:29 Connected to 127.0.0.1:2181
2019/11/10 15:25:29 authenticated: id=72066667419402356, timeout=10000
2019/11/10 15:25:29 re-submitting `0` credentials after reconnect
zookeeper connected

quit
➜  zooklient git:(master) ✗
```

# Help me to improve
If you find some bugs or want more functions, you can create an issue.   
I am glad to help.
