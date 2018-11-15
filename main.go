package main

import (
	"hadoop-fs/fs"
	"hadoop-fs/fs/config"
	// _ "net/http/pprof"
)

func main() {

	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()
	// cg := config.Config{}
	// cg.Attrtimeout = 1
	// cg.Hadoop.Host = "192.168.50.254"
	// cg.Hadoop.Port = 50070
	// cg.Hadoop.Username = "ming"
	// cg.Mountpoint = "/home/ming/golang/project/src/transfer-fs/test"
	// cg.Debug = true
	// cg.NotExistCacheTimeout = 200

	cg := config.ParseFromCmd()
	fs.Service(cg)

}
