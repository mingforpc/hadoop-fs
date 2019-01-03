package fs

import (
	"hadoop-fs/fs/config"
	"hadoop-fs/fs/controler"
	"hadoop-fs/fs/logger"
	"os"
	"os/signal"
	"syscall"

	"github.com/mingforpc/fuse-go/fuse"
	"github.com/mingforpc/fuse-go/fuse/mount"
	"github.com/mingforpc/fuse-go/fuse/util"
)

var pathManager = util.FusePathManager{}
var hadoopControler controler.HadoopController
var notExistManager = util.NotExistManager{}

// Service 服务开始
func Service(cg config.Config) {

	hadoopControler = controler.HadoopController{}
	hadoopControler.Init(false, cg.Hadoop.Host, cg.Hadoop.Port, cg.Hadoop.Username)

	notExistManager.Init(cg.NotExistCacheTimeout)

	pathManager.Init()

	opts := fuse.Opt{}
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
	opts.Setxattr = &setxattr
	opts.Getxattr = &getxattr
	opts.Listxattr = &listxattr
	opts.Removexattr = &removexattr

	// Hadoop不支持，暂时去掉
	// opts.Symlink = &symlink

	se := fuse.NewFuseSession(cg.Mountpoint, &opts, 1024)

	se.Debug = cg.Debug
	se.FuseConfig.AttrTimeout = cg.Attrtimeout

	err := mount.Mount(se)

	if err != nil {
		logger.Error.Println(err)
		return
	}

	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGQUIT)
	go exitSign(signalChan, se)

	se.FuseLoop()

	se.Close()

}

func umount(se *fuse.Session) {

	err := mount.Unmount(se.Mountpoint)
	if err != nil {
		logger.Error.Printf(":umount failed [%s], Please umount folder manually! \n", err)
	}

}

func exitSign(signalChan chan os.Signal, se *fuse.Session) {

	sign := <-signalChan

	logger.Info.Printf("Receive Sign[%s]\n", sign)

	umount(se)

}
