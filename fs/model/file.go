package model

import (
	"hadoop-fs/fs/util"
	"os/user"
	"strconv"
	"syscall"

	"github.com/mingforpc/fuse-go/fuse"
)

const TYPE_FILE = syscall.S_IFREG
const TYPE_DIR = syscall.S_IFDIR
const TYPE_SYMLINK = syscall.S_IFLNK

const (
	HADOOP_DIR     = "DIRECTORY"
	HADOOP_FILE    = "FILE"
	HADOOP_SYMLINK = "SYMLINK"
)

type FileModel struct {
	Name      string `json:"pathSuffix"`
	FileType  int
	StMode    uint
	StIno     uint32 `json:"fileId"`
	StDev     uint32
	StRdev    uint32
	StNlink   uint32
	StUid     uint
	StGid     uint
	StSize    int64 `json:"length"`
	StAtime   int64 `json:"accessTime"`
	StMtime   int64 `json:"modificationTime"`
	StCtime   int64
	StBlksize int32 `json:"blockSize"`
	StBlocks  int32

	HadoopOwner      string `json:"owner"`
	HadoopGroup      string `json:"group"`
	HadoopType       string `json:"type"`
	HadoopPermission string `json:"permission"`
	ChildrenNum      int    `json:"childrenNum"`
}

func (file *FileModel) WriteToStat(stat *syscall.Stat_t) {

	stat.Ino = uint64(file.StIno)
	stat.Mode = uint32(file.FileType) | uint32(file.StMode)

	stat.Uid = uint32(file.StUid)
	stat.Gid = uint32(file.StGid)

	stat.Nlink = uint64(file.StNlink)

	stat.Size = file.StSize

	stat.Blksize = int64(file.StBlksize)
	stat.Dev = uint64(file.StDev)
	stat.Rdev = uint64(file.StRdev)
	stat.Blocks = int64(file.StBlocks)

	stat.Atim = syscall.NsecToTimespec(file.StAtime)
	stat.Mtim = syscall.NsecToTimespec(file.StMtime)
	stat.Ctim = syscall.NsecToTimespec(file.StCtime)
}

// 当FileModel是由 WebHDFS 接口获取时，需要调用该接口对部分属性进行转换
func (file *FileModel) AdjustNormal() {

	file.StMtime = util.MsToNs(file.StMtime)

	switch file.HadoopType {
	case HADOOP_DIR:
		file.FileType = TYPE_DIR
		file.StSize = 4096
		file.StNlink = 2
		if file.StAtime == 0 {
			file.StAtime = file.StMtime
		}
	case HADOOP_FILE:
		file.FileType = TYPE_FILE
		file.StNlink = 1
	case HADOOP_SYMLINK:
		file.FileType = TYPE_SYMLINK
	}

	file.StCtime = file.StMtime

	mode, _ := strconv.ParseUint(file.HadoopPermission, 8, 16)

	file.StMode = uint(mode)

	// user, group
	fileUser, err := user.Lookup(file.HadoopOwner)
	if err != nil {
		fileUser, _ = user.Lookup("nobody")
	}
	uid, _ := strconv.Atoi(fileUser.Uid)
	file.StUid = uint(uid)

	fileGroup, err := user.LookupGroup(file.HadoopOwner)
	if err != nil {
		fileGroup, _ = user.LookupGroup("nogroup")
	}
	gid, _ := strconv.Atoi(fileGroup.Gid)
	file.StGid = uint(gid)
}

func (file *FileModel) ToFuseDirent() fuse.Dirent {
	ent := fuse.Dirent{}

	ent.Ino = uint64(file.StIno)
	ent.NameLen = uint32(len(file.Name))
	ent.DirType = uint32(file.StMode)
	ent.Name = file.Name

	return ent
}
