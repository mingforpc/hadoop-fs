package fs

import (
	"hadoop-fs/fs/controler"
	herr "hadoop-fs/fs/controler/hadoop_error"
	"hadoop-fs/fs/logger"
	"hadoop-fs/fs/util"
	"syscall"

	"github.com/mingforpc/fuse-go/fuse"
	"github.com/mingforpc/fuse-go/fuse/errno"
	"github.com/mingforpc/fuse-go/fuse/kernel"
)

// 统一的错误处理
func recoverError(res *int32) {
	if err := recover(); err != nil {
		switch err {
		case herr.NO_FOUND:
			*res = errno.ENOENT
		case herr.EEXIST:
			*res = errno.EEXIST
		case herr.EACCES:
			*res = errno.ENOENT
		case herr.EAGAIN:
			*res = errno.EAGAIN
		case herr.ENOTSUP:
			*res = errno.ENOTSUP
		default:
			*res = errno.ENOSYS
		}

	}
}

var getattr = func(req fuse.FuseReq, nodeid uint64, stat *syscall.Stat_t) (result int32) {

	defer recoverError(&result)

	path := PATH_MANAGER.Get(nodeid)

	logger.Trace.Printf("getattr: path[%s] \n", path)

	if path == "/" {

		rootfile := controler.ROOT.GetRoot(req)

		rootfile.WriteToStat(stat)

	} else {

		file, err := HADOOP.GetFileStatus(path)

		if err != nil {
			panic(err)
		}

		file.AdjustNormal()
		file.WriteToStat(stat)

	}

	result = errno.SUCCESS

	return result
}

var opendir = func(req fuse.FuseReq, nodeid uint64, fi *fuse.FuseFileInfo) int32 {

	return errno.SUCCESS
}

var readdir = func(req fuse.FuseReq, nodeid uint64, size uint32, offset uint64, fi fuse.FuseFileInfo) (fileList []kernel.FuseDirent, result int32) {

	defer recoverError(&result)

	path := PATH_MANAGER.Get(nodeid)

	logger.Trace.Printf("readdir: path[%s] \n", path)

	fileList = make([]kernel.FuseDirent, 0)

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

	result = errno.SUCCESS

	return fileList, result

}

var release = func(req fuse.FuseReq, nodeid uint64, fi fuse.FuseFileInfo) (result int32) {

	defer recoverError(&result)

	path := PATH_MANAGER.Get(nodeid)

	if path != "/" {
		logger.Trace.Printf("release: nodeid[%d], path[%s]\n", nodeid, path)
	}

	result = errno.SUCCESS
	return result
}

var lookup = func(req fuse.FuseReq, parentId uint64, name string, stat *syscall.Stat_t, generation *uint64) (result int32) {

	defer recoverError(&result)

	parentPath := PATH_MANAGER.Get(parentId)

	logger.Trace.Printf("parentId[%d], parentPath[%s], name[%s]\n", parentId, parentPath, name)

	filePath := util.MergePath(parentPath, name)

	if NOT_EXIST_FILE_CACHE.IsNotExist(filePath) == false {
		// 文件不存在
		panic(herr.NO_FOUND)
		// return errno.ENOENT
	}

	file, err := HADOOP.GetFileStatus(filePath)

	if err != nil {
		// 不存在的文件会缓存 NOT_EXIST_FILE_CACHE 中的秒数
		NOT_EXIST_FILE_CACHE.Insert(filePath, NOT_EXIST_FILE_CACHE.NegativeTimeout)
		// return errno.ENOENT
		panic(herr.NO_FOUND)
	}

	file.AdjustNormal()

	file.WriteToStat(stat)

	PATH_MANAGER.Insert(uint64(file.StIno), filePath)

	// TODO:
	*generation = 1

	result = errno.SUCCESS
	return result
}

var open = func(req fuse.FuseReq, nodeid uint64, fi *fuse.FuseFileInfo) int32 {

	return errno.SUCCESS
}

var read = func(req fuse.FuseReq, nodeid uint64, size uint32, offset uint64, fi fuse.FuseFileInfo) (content []byte, result int32) {

	defer recoverError(&result)

	path := PATH_MANAGER.Get(nodeid)

	logger.Info.Printf("nodeid[%d], path[%s], size[%d], offset[%d], fi[%+v] \n", nodeid, path, size, offset, fi)

	if path == "" {
		// 文件不存在
		return nil, errno.ENOENT
	}

	content, err := HADOOP.Read(path, offset, size, 0)

	if err != nil && err != herr.EOF {
		// TODO: 出错
		panic(err)
	}

	result = errno.SUCCESS
	return content, result
}

var mkdir = func(req fuse.FuseReq, parentid uint64, name string, mode uint32) (stat *fuse.FuseStat, result int32) {

	defer recoverError(&result)

	path := PATH_MANAGER.Get(parentid)
	filePath := util.MergePath(path, name)

	modeStr := util.ModeToStr(mode)

	success, err := HADOOP.MakeDir(filePath, modeStr)

	if err != nil {
		panic(err)
	} else if !success {
		panic(herr.EACCES)
	}

	file, err := HADOOP.GetFileStatus(filePath)

	if err != nil {
		panic(err)
	}

	stat = &fuse.FuseStat{}

	file.AdjustNormal()
	file.WriteToStat(&stat.Stat)

	stat.Nodeid = uint64(file.StIno)
	stat.Generation = 1

	// 加入到路径的缓存
	PATH_MANAGER.Insert(stat.Nodeid, filePath)
	// 删除不存在文件缓存
	NOT_EXIST_FILE_CACHE.Delete(filePath)

	return stat, errno.SUCCESS
}

var create = func(req fuse.FuseReq, parentid uint64, name string, mode uint32, fi *fuse.FuseFileInfo) (stat *fuse.FuseStat, result int32) {

	defer recoverError(&result)

	logger.Trace.Printf(" parentid[%d], name[%s], mode[%d], fi[%+v] \n", parentid, name, mode, fi)

	path := PATH_MANAGER.Get(parentid)

	if path == "" {
		// 父目录不在路径缓存中
		return nil, errno.ENOENT
	}

	filePath := util.MergePath(path, name)

	modeStr := util.ModeToStr(mode)

	err := HADOOP.Create(filePath, modeStr)

	if err != nil {
		panic(err)
	}

	file, err := HADOOP.GetFileStatus(filePath)

	if err != nil {
		panic(err)
	}

	stat = &fuse.FuseStat{}

	file.AdjustNormal()
	file.WriteToStat(&stat.Stat)

	stat.Nodeid = uint64(file.StIno)
	stat.Generation = 1

	// 加入到路径的缓存
	PATH_MANAGER.Insert(stat.Nodeid, filePath)

	// 删除不存在文件缓存
	NOT_EXIST_FILE_CACHE.Delete(filePath)

	return stat, errno.SUCCESS
}

var setattr = func(req fuse.FuseReq, nodeid uint64, attr *syscall.Stat_t, toSet uint32) (result int32) {

	defer recoverError(&result)

	filepath := PATH_MANAGER.Get(nodeid)

	logger.Trace.Printf("nodeid[%d], filepath[%s], attr[%+v], toSet[%d]\n", nodeid, filepath, attr, toSet)

	if filepath == "" {
		// 文件不在路径缓存中
		return errno.ENOENT
	}

	var atime int64 = -1
	var mtime int64 = -1

	if toSet&fuse.FUSE_SET_ATTR_ATIME > 0 {
		// 设置文件atime

		atime = util.NsToMs(syscall.TimespecToNsec(attr.Atim))
	}
	if toSet&fuse.FUSE_SET_ATTR_MTIME > 0 {
		// 设置文件mtime
		mtime = util.NsToMs(syscall.TimespecToNsec(attr.Mtim))
	}

	if atime > 0 || mtime > 0 {
		logger.Trace.Printf("atime[%d], mtime[%d] \n", atime, mtime)

		err := HADOOP.ModificationTime(filepath, atime, mtime)
		if err != nil {
			panic(err)
		}

	}

	if toSet&fuse.FUSE_SET_ATTR_MODE > 0 {
		// 设置文件的permission

		modeStr := util.ModeToStr(attr.Mode)
		err := HADOOP.SetPermission(filepath, modeStr)

		if err != nil {
			panic(err)
		}
	}

	// 由于Hadoop中没有ctime所以忽略
	// 忽略UID, GID，因为由启动的参数决定的

	return errno.SUCCESS
}

var write = func(req fuse.FuseReq, nodeid uint64, buf []byte, offset uint64, fi fuse.FuseFileInfo) (size uint32, result int32) {

	defer recoverError(&result)

	filepath := PATH_MANAGER.Get(nodeid)

	logger.Trace.Printf("nodeid[%d], filepath[%s], buf[%s], offset[%d], fi[%+v]\n", nodeid, filepath, buf, offset, fi)

	file, err := HADOOP.GetFileStatus(filepath)

	if err != nil {
		panic(err)
	}

	file.AdjustNormal()

	if offset == uint64(file.StSize) {
		// 直接追加
		err = HADOOP.AppendFile(filepath, buf)
	} else {
		// 先Truncate到offset的位置，再追加
		success := false
		success, err = HADOOP.TruncateFile(filepath, int64(offset))

		if err != nil {
			panic(err)
		} else if !success {
			panic(herr.EACCES)
		} else {
			err = HADOOP.AppendFile(filepath, buf)
		}
	}

	if err != nil {
		panic(err)
	}

	size = uint32(len(buf))

	return size, errno.SUCCESS
}

func _rmFileOrDir(req fuse.FuseReq, parentid uint64, name string) (result int32) {
	defer recoverError(&result)

	parentPath := PATH_MANAGER.Get(parentid)

	logger.Trace.Printf("parentid[%d], parentPath[%s], name[%s]\n", parentid, parentPath, name)

	filePath := util.MergePath(parentPath, name)

	file, err := HADOOP.GetFileStatus(filePath)
	if err != nil {
		panic(err)
	}
	file.AdjustNormal()

	success, err := HADOOP.Delete(filePath)
	if err != nil {
		panic(err)
	} else if !success {
		panic(herr.EACCES)
	}

	PATH_MANAGER.Delete(uint64(file.StIno))

	return errno.SUCCESS
}

// 删除文件函数
var unlink = func(req fuse.FuseReq, parentid uint64, name string) (result int32) {
	return _rmFileOrDir(req, parentid, name)
}

// 删除文件夹函数
var rmdir = func(req fuse.FuseReq, parentid uint64, name string) (result int32) {
	return _rmFileOrDir(req, parentid, name)
}

// 重命名文件
var rename = func(req fuse.FuseReq, parentid uint64, name string, newparentid uint64, newname string) (result int32) {

	defer recoverError(&result)

	parentPath := PATH_MANAGER.Get(parentid)
	newParentPath := PATH_MANAGER.Get(newparentid)

	logger.Trace.Printf("rename: parentid[%d], parentPath[%s], name[%s], newparentid[%d], newParentPath[%s], newname[%s]\n", parentid, parentPath, name, newparentid, newParentPath, newname)

	filePath := util.MergePath(parentPath, name)
	newFilePath := util.MergePath(newParentPath, newname)

	// 获取文件信息
	file, err := HADOOP.GetFileStatus(filePath)
	if err != nil {
		panic(err)
	}
	file.AdjustNormal()

	// Rename 文件
	success, err := HADOOP.Rename(filePath, newFilePath)
	if err != nil {
		panic(err)
	} else if !success {
		panic(herr.EACCES)
	}

	// 获取Rename后文件的信息
	newfile, err := HADOOP.GetFileStatus(newFilePath)
	if err != nil {
		panic(err)
	}
	newfile.AdjustNormal()

	// 缓存管理
	PATH_MANAGER.Delete(uint64(file.StIno))
	PATH_MANAGER.Insert(uint64(newfile.StIno), newFilePath)
	NOT_EXIST_FILE_CACHE.Delete(newFilePath)

	return errno.SUCCESS
}

// 设置文件额外属性
var setxattr = func(req fuse.FuseReq, nodeid uint64, name string, value string, flags uint32) (result int32) {

	defer recoverError(&result)

	filepath := PATH_MANAGER.Get(nodeid)

	logger.Trace.Printf("setxattr: nodeid[%d], filepath[%s], name[%s], value[%s], flags[%d]\n", nodeid, filepath, name, value, flags)

	strFlag := "CREATE"

	switch flags {
	case 0:
	case fuse.XATTR_CREATE:
	case fuse.XATTR_REPLACE:
		strFlag = "REPLACE"
	}

	err := HADOOP.Setxattr(filepath, name, value, strFlag)

	if err != nil {
		if err == herr.EEXIST {
			// Xattr已经存在要用replace
			err = HADOOP.Setxattr(filepath, name, value, "REPLACE")
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}

	return errno.SUCCESS
}

// Hadoop不支持
var symlink = func(req fuse.FuseReq, parentid uint64, link string, name string) (stat *fuse.FuseStat, result int32) {

	defer recoverError(&result)

	parentPath := PATH_MANAGER.Get(parentid)

	logger.Trace.Printf("symlink: parentid[%d], parentPath[%s], link[%s], name[%s]\n", parentid, parentPath, link, name)

	srcPath := util.MergePath(parentPath, link)
	symlinkPath := util.MergePath(parentPath, name)

	err := HADOOP.CreateSymlink(srcPath, symlinkPath)
	if err != nil {
		panic(err)
	}

	symlinkFile, err := HADOOP.GetFileStatus(symlinkPath)
	if err != nil {
		panic(err)
	}
	symlinkFile.AdjustNormal()

	stat = &fuse.FuseStat{}
	symlinkFile.WriteToStat(&stat.Stat)
	stat.Nodeid = uint64(symlinkFile.StIno)
	stat.Generation = 1

	// 加入到路径的缓存
	PATH_MANAGER.Insert(stat.Nodeid, symlinkPath)

	return stat, errno.SUCCESS
}
