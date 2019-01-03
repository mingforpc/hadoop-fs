package fs

import (
	"bytes"
	"hadoop-fs/fs/controler"
	herr "hadoop-fs/fs/controler/hadoop_error"
	"hadoop-fs/fs/logger"
	"hadoop-fs/fs/util"
	"syscall"

	"github.com/mingforpc/fuse-go/fuse"
	"github.com/mingforpc/fuse-go/fuse/errno"
)

// 统一的错误处理
func recoverError(res *int32) {
	if err := recover(); err != nil {
		switch err {
		case herr.ErrNoFound:
			*res = errno.ENOENT
		case herr.ErrExist:
			*res = errno.EEXIST
		case herr.ErrAccess:
			*res = errno.ENOENT
		case herr.ErrAgain:
			*res = errno.EAGAIN
		case herr.ErrNotsup:
			*res = errno.ENOTSUP
		case herr.ErrRange:
			*res = errno.ERANGE
		case herr.ErrNoAttr:
			*res = errno.ENOATTR
		default:
			*res = errno.ENOSYS
		}

	}
}

var getattr = func(req fuse.Req, nodeid uint64) (fsStat *fuse.FileStat, result int32) {

	defer recoverError(&result)

	path := pathManager.Get(nodeid)

	fsStat = &fuse.FileStat{}

	if path == "/" {

		rootfile := controler.ROOT.GetRoot(req)

		rootfile.WriteToStat(&fsStat.Stat)

	} else {

		file, err := hadoopControler.GetFileStatus(path)

		if err != nil {
			panic(err)
		}

		file.AdjustNormal()
		file.WriteToStat(&fsStat.Stat)

	}

	result = errno.SUCCESS

	return fsStat, result
}

var opendir = func(req fuse.Req, nodeid uint64, fi *fuse.FileInfo) int32 {

	return errno.SUCCESS
}

var readdir = func(req fuse.Req, nodeid uint64, size uint32, offset uint64, fi fuse.FileInfo) (fileList []fuse.Dirent, result int32) {

	defer recoverError(&result)

	path := pathManager.Get(nodeid)

	fileList = make([]fuse.Dirent, 0)

	// 记录fileList中的文件数量
	fileCount := uint32(0)

	// 假定一个文件占32，这个就能有一个大概的最大数量
	fileMaxCount := size / 32

	// 假定一个文件占32, 这个就能有一个请求大概的偏移量
	fileReqOffset := offset / 32

	if fileReqOffset < 2 {
		current := fuse.Dirent{NameLen: uint32(len(".")), Ino: nodeid, Off: 0, Name: "."}
		prev := fuse.Dirent{NameLen: uint32(len("..")), Ino: nodeid, Off: 0, Name: ".."}

		fileList = make([]fuse.Dirent, 2)

		fileList[0] = current
		fileList[1] = prev

		fileCount += 2
	}

	lastPathSuffix := ""

	// 已经有2个文件"."和"..", 记录当前的文件偏移量
	fileOffset := uint64(2)

	for {
		remoteFiles, remain, _ := hadoopControler.List(path, lastPathSuffix)

		for _, val := range remoteFiles {

			file := val
			fileOffset++

			if fileOffset > fileReqOffset {
				file.AdjustNormal()

				fileList = append(fileList, file.ToFuseDirent())

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

var release = func(req fuse.Req, nodeid uint64, fi fuse.FileInfo) (result int32) {

	defer recoverError(&result)

	path := pathManager.Get(nodeid)

	if path != "/" {
		logger.Trace.Printf("release: nodeid[%d], path[%s]\n", nodeid, path)
	}

	result = errno.SUCCESS
	return result
}

var lookup = func(req fuse.Req, parentId uint64, name string) (fsStat *fuse.FileStat, result int32) {

	defer recoverError(&result)

	parentPath := pathManager.Get(parentId)
	filePath := util.MergePath(parentPath, name)

	if notExistManager.IsNotExist(filePath) == false {
		// 文件不存在
		panic(herr.ErrNoFound)
		// return errno.ENOENT
	}

	file, err := hadoopControler.GetFileStatus(filePath)

	if err != nil {
		// 不存在的文件会缓存 notExistManager 中的秒数
		notExistManager.Set(filePath, notExistManager.NegativeTimeout)
		// return errno.ENOENT
		panic(herr.ErrNoFound)
	}

	file.AdjustNormal()

	fsStat = &fuse.FileStat{}

	fsStat.Nodeid = uint64(file.StIno)
	file.WriteToStat(&fsStat.Stat)

	pathManager.Set(uint64(file.StIno), filePath)

	// TODO:
	fsStat.Generation = 1

	result = errno.SUCCESS
	return fsStat, result
}

var open = func(req fuse.Req, nodeid uint64, fi *fuse.FileInfo) int32 {

	return errno.SUCCESS
}

var read = func(req fuse.Req, nodeid uint64, size uint32, offset uint64, fi fuse.FileInfo) (content []byte, result int32) {

	defer recoverError(&result)

	path := pathManager.Get(nodeid)

	logger.Info.Printf("nodeid[%d], path[%s], size[%d], offset[%d], fi[%+v] \n", nodeid, path, size, offset, fi)

	if path == "" {
		// 文件不存在
		return nil, errno.ENOENT
	}

	content, err := hadoopControler.Read(path, offset, size, 0)

	if err != nil && err != herr.ErrEOF {
		// TODO: 出错
		panic(err)
	}

	result = errno.SUCCESS
	return content, result
}

var mkdir = func(req fuse.Req, parentid uint64, name string, mode uint32) (stat *fuse.FileStat, result int32) {

	defer recoverError(&result)

	path := pathManager.Get(parentid)
	filePath := util.MergePath(path, name)

	modeStr := util.ModeToStr(mode)

	success, err := hadoopControler.MakeDir(filePath, modeStr)

	if err != nil {
		panic(err)
	} else if !success {
		panic(herr.ErrAccess)
	}

	file, err := hadoopControler.GetFileStatus(filePath)

	if err != nil {
		panic(err)
	}

	stat = &fuse.FileStat{}

	file.AdjustNormal()
	file.WriteToStat(&stat.Stat)

	stat.Nodeid = uint64(file.StIno)
	stat.Generation = 1

	// 加入到路径的缓存
	pathManager.Set(stat.Nodeid, filePath)
	// 删除不存在文件缓存
	notExistManager.Del(filePath)

	return stat, errno.SUCCESS
}

var create = func(req fuse.Req, parentid uint64, name string, mode uint32, fi *fuse.FileInfo) (stat *fuse.FileStat, result int32) {

	defer recoverError(&result)

	logger.Trace.Printf(" parentid[%d], name[%s], mode[%d], fi[%+v] \n", parentid, name, mode, fi)

	path := pathManager.Get(parentid)

	if path == "" {
		// 父目录不在路径缓存中
		return nil, errno.ENOENT
	}

	filePath := util.MergePath(path, name)

	modeStr := util.ModeToStr(mode)

	err := hadoopControler.Create(filePath, modeStr)

	if err != nil {
		panic(err)
	}

	file, err := hadoopControler.GetFileStatus(filePath)

	if err != nil {
		panic(err)
	}

	stat = &fuse.FileStat{}

	file.AdjustNormal()
	file.WriteToStat(&stat.Stat)

	stat.Nodeid = uint64(file.StIno)
	stat.Generation = 1

	// 加入到路径的缓存
	pathManager.Set(stat.Nodeid, filePath)

	// 删除不存在文件缓存
	notExistManager.Del(filePath)

	return stat, errno.SUCCESS
}

var setattr = func(req fuse.Req, nodeid uint64, attr fuse.FileStat, toSet uint32) (result int32) {

	defer recoverError(&result)

	filepath := pathManager.Get(nodeid)

	logger.Trace.Printf("nodeid[%d], filepath[%s], attr[%+v], toSet[%d]\n", nodeid, filepath, attr, toSet)

	if filepath == "" {
		// 文件不在路径缓存中
		return errno.ENOENT
	}

	var atime int64 = -1
	var mtime int64 = -1

	if toSet&fuse.FuseSetAttrAtime > 0 {
		// 设置文件atime

		atime = util.NsToMs(syscall.TimespecToNsec(attr.Stat.Atim))
	}
	if toSet&fuse.FuseSetAttrMtime > 0 {
		// 设置文件mtime
		mtime = util.NsToMs(syscall.TimespecToNsec(attr.Stat.Mtim))
	}

	if atime > 0 || mtime > 0 {
		logger.Trace.Printf("atime[%d], mtime[%d] \n", atime, mtime)

		err := hadoopControler.ModificationTime(filepath, atime, mtime)
		if err != nil {
			panic(err)
		}

	}

	if toSet&fuse.FuseSetAttrMode > 0 {
		// 设置文件的permission

		modeStr := util.ModeToStr(attr.Stat.Mode)
		err := hadoopControler.SetPermission(filepath, modeStr)

		if err != nil {
			panic(err)
		}
	}

	// 由于hadoopControler中没有ctime所以忽略
	// 忽略UID, GID，因为由启动的参数决定的

	return errno.SUCCESS
}

var write = func(req fuse.Req, nodeid uint64, buf []byte, offset uint64, fi fuse.FileInfo) (size uint32, result int32) {

	defer recoverError(&result)

	filepath := pathManager.Get(nodeid)

	logger.Trace.Printf("nodeid[%d], filepath[%s], buf[%s], offset[%d], fi[%+v]\n", nodeid, filepath, buf, offset, fi)

	file, err := hadoopControler.GetFileStatus(filepath)

	if err != nil {
		panic(err)
	}

	file.AdjustNormal()

	if offset == uint64(file.StSize) {
		// 直接追加
		err = hadoopControler.AppendFile(filepath, buf)
	} else {
		// 先Truncate到offset的位置，再追加
		success := false
		success, err = hadoopControler.TruncateFile(filepath, int64(offset))

		if err != nil {
			panic(err)
		} else if !success {
			panic(herr.ErrAccess)
		} else {
			err = hadoopControler.AppendFile(filepath, buf)
		}
	}

	if err != nil {
		panic(err)
	}

	size = uint32(len(buf))

	return size, errno.SUCCESS
}

func _rmFileOrDir(req fuse.Req, parentid uint64, name string) (result int32) {
	defer recoverError(&result)

	parentPath := pathManager.Get(parentid)

	logger.Trace.Printf("parentid[%d], parentPath[%s], name[%s]\n", parentid, parentPath, name)

	filePath := util.MergePath(parentPath, name)

	file, err := hadoopControler.GetFileStatus(filePath)
	if err != nil {
		panic(err)
	}
	file.AdjustNormal()

	success, err := hadoopControler.Delete(filePath)
	if err != nil {
		panic(err)
	} else if !success {
		panic(herr.ErrAccess)
	}

	pathManager.Del(uint64(file.StIno))

	return errno.SUCCESS
}

// 删除文件函数
var unlink = func(req fuse.Req, parentid uint64, name string) (result int32) {
	return _rmFileOrDir(req, parentid, name)
}

// 删除文件夹函数
var rmdir = func(req fuse.Req, parentid uint64, name string) (result int32) {
	return _rmFileOrDir(req, parentid, name)
}

// 重命名文件
var rename = func(req fuse.Req, parentid uint64, name string, newparentid uint64, newname string) (result int32) {

	defer recoverError(&result)

	parentPath := pathManager.Get(parentid)
	newParentPath := pathManager.Get(newparentid)

	logger.Trace.Printf("rename: parentid[%d], parentPath[%s], name[%s], newparentid[%d], newParentPath[%s], newname[%s]\n", parentid, parentPath, name, newparentid, newParentPath, newname)

	filePath := util.MergePath(parentPath, name)
	newFilePath := util.MergePath(newParentPath, newname)

	// 获取文件信息
	file, err := hadoopControler.GetFileStatus(filePath)
	if err != nil {
		panic(err)
	}
	file.AdjustNormal()

	// Rename 文件
	success, err := hadoopControler.Rename(filePath, newFilePath)
	if err != nil {
		panic(err)
	} else if !success {
		panic(herr.ErrAccess)
	}

	// 获取Rename后文件的信息
	newfile, err := hadoopControler.GetFileStatus(newFilePath)
	if err != nil {
		panic(err)
	}
	newfile.AdjustNormal()

	// 缓存管理
	pathManager.Del(uint64(file.StIno))
	pathManager.Set(uint64(newfile.StIno), newFilePath)
	notExistManager.Del(newFilePath)

	return errno.SUCCESS
}

// 设置文件额外属性
var setxattr = func(req fuse.Req, nodeid uint64, name string, value string, flags uint32) (result int32) {

	defer recoverError(&result)

	filepath := pathManager.Get(nodeid)

	logger.Trace.Printf("setxattr: nodeid[%d], filepath[%s], name[%s], value[%s], flags[%d]\n", nodeid, filepath, name, value, flags)

	strFlag := "CREATE"

	switch flags {
	case 0:
	case fuse.XattrCreate:
	case fuse.XattrReplace:
		strFlag = "REPLACE"
	}

	err := hadoopControler.Setxattr(filepath, name, value, strFlag)

	if err != nil {
		if err == herr.ErrExist {
			// Xattr已经存在要用replace
			err = hadoopControler.Setxattr(filepath, name, value, "REPLACE")
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}

	return errno.SUCCESS
}

// 获取指定名字的文件额外属性值
var getxattr = func(req fuse.Req, nodeid uint64, name string, size uint32) (value string, result int32) {

	defer recoverError(&result)

	filepath := pathManager.Get(nodeid)
	logger.Trace.Printf("getxattr: nodeid[%d], filepath[%s], name[%s], size[%d]\n", nodeid, filepath, name, size)

	value, err := hadoopControler.Getxattr(filepath, name)

	if err != nil {
		panic(err)
	}

	if size > 0 && uint32(len(value)) > size {
		panic(herr.ErrRange)
	}

	return value, errno.SUCCESS
}

var listxattr = func(req fuse.Req, nodeid uint64, size uint32) (list string, result int32) {
	defer recoverError(&result)

	filepath := pathManager.Get(nodeid)
	logger.Trace.Printf("listxattr: nodeid[%d], filepath[%s],  size[%d]\n", nodeid, filepath, size)

	attrs, err := hadoopControler.Listxattr(filepath)

	if err != nil {
		panic(err)
	}

	buf := bytes.NewBuffer(nil)
	length := len(attrs)
	for i := 0; i < length; i++ {
		buf.Write([]byte(attrs[i].Name))
		if i < length-1 {
			buf.WriteByte(byte(0))
		}

	}
	list = buf.String()

	if size > 0 && uint32(len(list)) > size {
		panic(herr.ErrRange)
	}

	return list, errno.SUCCESS
}

var removexattr = func(req fuse.Req, nodeid uint64, name string) (result int32) {
	defer recoverError(&result)

	filepath := pathManager.Get(nodeid)
	logger.Trace.Printf("removexattr: nodeid[%d], filepath[%s],  name[%s]\n", nodeid, filepath, name)

	err := hadoopControler.Removexattr(filepath, name)

	if err != nil {
		panic(err)
	}

	return errno.SUCCESS
}

// hadoopControler不支持
var symlink = func(req fuse.Req, parentid uint64, link string, name string) (stat *fuse.FileStat, result int32) {

	defer recoverError(&result)

	parentPath := pathManager.Get(parentid)

	logger.Trace.Printf("symlink: parentid[%d], parentPath[%s], link[%s], name[%s]\n", parentid, parentPath, link, name)

	srcPath := util.MergePath(parentPath, link)
	symlinkPath := util.MergePath(parentPath, name)

	err := hadoopControler.CreateSymlink(srcPath, symlinkPath)
	if err != nil {
		panic(err)
	}

	symlinkFile, err := hadoopControler.GetFileStatus(symlinkPath)
	if err != nil {
		panic(err)
	}
	symlinkFile.AdjustNormal()

	stat = &fuse.FileStat{}
	symlinkFile.WriteToStat(&stat.Stat)
	stat.Nodeid = uint64(symlinkFile.StIno)
	stat.Generation = 1

	// 加入到路径的缓存
	pathManager.Set(stat.Nodeid, symlinkPath)

	return stat, errno.SUCCESS
}
