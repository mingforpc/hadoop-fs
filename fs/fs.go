package fs

import (
	"hadoop-fs/fs/cache"
	"hadoop-fs/fs/config"
	"hadoop-fs/fs/controler"
	"hadoop-fs/fs/logger"
	"os"
	"os/signal"
	"syscall"

	"github.com/mingforpc/fuse-go/fuse"
	"github.com/mingforpc/fuse-go/fuse/mount"
)

var PATH_MANAGER = cache.FusePathManager{}
var HADOOP controler.HadoopController
var NOT_EXIST_FILE_CACHE cache.NotExistCache

// 服务开始
func Service(cg config.Config) {

	HADOOP = controler.HadoopController{}
	HADOOP.Init(false, cg.Hadoop.Host, cg.Hadoop.Port, cg.Hadoop.Username)

	NOT_EXIST_FILE_CACHE = cache.NotExistCache{}
	NOT_EXIST_FILE_CACHE.Init()
	NOT_EXIST_FILE_CACHE.NegativeTimeout = cg.NotExistCacheTimeout

	PATH_MANAGER.Init()

	opts := fuse.FuseOpt{}
	opts.Getattr = &getattr
	opts.Opendir = &opendir
	opts.Readdir = &readdir
	opts.Releasedir = &release
	opts.Release = &release
	opts.Lookup = &lookup
	opts.Open = &open
	opts.Read = &read
	opts.Mkdir = &mkdir
	opts.Create = &create
	opts.Setattr = &setattr
	opts.Write = &write
	opts.Unlink = &unlink
	opts.Rmdir = &rmdir
	opts.Rename = &rename

	se := fuse.FuseSession{}

	se.Init(cg.Mountpoint, &opts)
	se.Debug = cg.Debug
	se.FuseConfig.AttrTimeout = cg.Attrtimeout

	err := mount.Mount(&se)

	if err != nil {
		logger.Error.Println(err)
		return
	}

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
	go exitSign(signalChan, &se)

	defer umount(&se)

	se.FuseLoop()

}

func umount(se *fuse.FuseSession) {

	err := mount.Unmount(se.Mountpoint)
	logger.Error.Printf("umount failed [%s], Please umount folder manually! \n", err)

}

func exitSign(signalChan chan os.Signal, se *fuse.FuseSession) {

	sign := <-signalChan

	logger.Info.Printf("Receive Sign[%s]\n", sign)

	se.Close()

}
