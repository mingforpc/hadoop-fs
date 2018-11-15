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
	"github.com/mingforpc/fuse-go/fuse/errno"
	"github.com/mingforpc/fuse-go/fuse/kernel"
	"github.com/mingforpc/fuse-go/fuse/mount"
)

var PATH_MANAGER = cache.FusePathManager{}
var HADOOP controler.HadoopController
var NOT_EXIST_FILE_CACHE cache.NotExistCache

var getattr = func(req fuse.FuseReq, nodeid uint64, stat *syscall.Stat_t) int32 {

	path := PATH_MANAGER.Get(nodeid)

	if path == "/" {

		rootfile := controler.ROOT.GetRoot(req)

		rootfile.WriteToStat(stat)

	} else {

		file, err := HADOOP.GetFileStatus(path)

		if err != nil {
			return errno.ENOENT
		}

		file.AdjustNormal()
		file.WriteToStat(stat)

	}

	return errno.SUCCESS
}

var opendir = func(req fuse.FuseReq, nodeid uint64, fi *fuse.FuseFileInfo) int32 {

	return errno.SUCCESS
}

var readdir = func(req fuse.FuseReq, nodeid uint64, size uint32, offset uint64, fi fuse.FuseFileInfo) (int32, []kernel.FuseDirent) {

	path := PATH_MANAGER.Get(nodeid)

	var fileList []kernel.FuseDirent = make([]kernel.FuseDirent, 0)

	// 记录fileList中的文件数量
	fileCount := uint32(0)

	// 假定一个文件占32，这个就能有一个大概的最大数量
	fileMaxCount := size / 32

	// 假定一个文件占32, 这个就能有一个请求大概的偏移量
	fileReqOffset := offset / 32

	if fileReqOffset < 2 {
		current := kernel.FuseDirent{NameLen: uint32(len(".")), Ino: nodeid, Off: 0, Name: "."}
		prev := kernel.FuseDirent{NameLen: uint32(len("..")), Ino: nodeid, Off: 0, Name: ".."}

		fileList = make([]kernel.FuseDirent, 2)

		fileList[0] = current
		fileList[1] = prev

		fileCount += 2
	}

	lastPathSuffix := ""

	// 已经有2个文件"."和"..", 记录当前的文件偏移量
	fileOffset := uint64(2)

	for {
		remoteFiles, remain, err := HADOOP.List(path, lastPathSuffix)

		logger.Trace.Printf("%+v, remain[%d], err[%s]\n", remoteFiles, remain, err)

		for i, _ := range remoteFiles {

			fileOffset++

			if fileOffset > fileReqOffset {
				remoteFiles[i].AdjustNormal()

				fileList = append(fileList, remoteFiles[i].ToFuseDirent())

				fileCount++

				// 判断是否超出文件数量的限制
				if fileCount >= fileMaxCount {
					break
				}
			}

		}

		if remain == 0 || fileCount >= fileMaxCount {
			break
		} else if remain > 0 {
			lastIndex := len(remoteFiles) - 1
			lastPathSuffix = remoteFiles[lastIndex].Name
		}
	}

	return errno.SUCCESS, fileList

}

var releasedir = func(req fuse.FuseReq, nodeid uint64, fi fuse.FuseFileInfo) int32 {
	return errno.SUCCESS
}

var lookup = func(req fuse.FuseReq, parentId uint64, name string, stat *syscall.Stat_t, generation *uint64) int32 {

	logger.Trace.Printf("parentId[%d], name[%s]\n", parentId, name)

	parentPath := PATH_MANAGER.Get(parentId)

	var filePath string
	if parentPath != "" && parentPath[len(parentPath)-1] != '/' {
		filePath = parentPath + "/" + name
	} else {
		filePath = parentPath + name
	}

	if NOT_EXIST_FILE_CACHE.IsNotExist(filePath) == false {
		// 文件不存在
		return errno.ENOENT
	}

	file, err := HADOOP.GetFileStatus(filePath)

	if err != nil {
		// 不存在的文件会缓存 NOT_EXIST_FILE_CACHE 中的秒数
		NOT_EXIST_FILE_CACHE.Insert(filePath, NOT_EXIST_FILE_CACHE.NegativeTimeout)
		return errno.ENOENT
	}

	file.AdjustNormal()

	file.WriteToStat(stat)

	PATH_MANAGER.Insert(uint64(file.StIno), filePath)

	// TODO:
	*generation = 1

	return errno.SUCCESS
}

var open = func(req fuse.FuseReq, nodeid uint64, fi *fuse.FuseFileInfo) int32 {

	return errno.SUCCESS
}

var read = func(req fuse.FuseReq, nodeid uint64, size uint32, offset uint64, fi fuse.FuseFileInfo) ([]byte, int32) {

	path := PATH_MANAGER.Get(nodeid)

	logger.Info.Printf("nodeid[%d], path[%s], size[%d], offset[%d], fi[%+v] \n", nodeid, path, size, offset, fi)

	if path == "" {
		// 文件不存在
		return nil, errno.ENOENT
	}

	content, err := HADOOP.Read(path, offset, size, 0)

	if err != nil && err != controler.EOF {
		// TODO: 出错，暂时当文件不存在处理
		return nil, errno.ENOENT
	}

	return content, errno.SUCCESS
}

func Service(cg config.Config) {

	HADOOP = controler.HadoopController{}
	HADOOP.Init(false, cg.Hadoop.Host, cg.Hadoop.Port)

	NOT_EXIST_FILE_CACHE = cache.NotExistCache{}
	NOT_EXIST_FILE_CACHE.Init()
	NOT_EXIST_FILE_CACHE.NegativeTimeout = cg.NotExistCacheTimeout

	PATH_MANAGER.Init()

	opts := fuse.FuseOpt{}
	opts.Getattr = &getattr
	opts.Opendir = &opendir
	opts.Readdir = &readdir
	opts.Releasedir = &releasedir
	opts.Lookup = &lookup
	opts.Open = &open
	opts.Read = &read

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
