package main

import (
	"hadoop-fs/fs"
	"hadoop-fs/fs/config"
)

func main() {

	cg := config.ParseFromCmd()
	fs.Service(cg)

}
