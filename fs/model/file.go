package model

import (
	"hadoop-fs/fs/util"
	"os/user"
	"strconv"
	"syscall"

	"github.com/mingforpc/fuse-go/fuse"
)

// 文件类型
const (
	TypeFile    = syscall.S_IFREG
	TypeDir     = syscall.S_IFDIR
	TypeSymlink = syscall.S_IFLNK
)

// Hadoop中的文件类型
const (
	HadoopDir     = "DIRECTORY"
	HadoopFile    = "FILE"
	HadoopSymlink = "SYMLINK"
)

// FileModel 保存文件信息的类
type FileModel struct {
	Name      string `json:"pathSuffix"`
	FileType  int
	StMode    uint
	StIno     uint32 `json:"fileId"`
	StDev     uint32
	StRdev    uint32
	StNlink   uint32
	StUID     uint
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

// WriteToStat 将FileModel中的信息写入stat中
func (file *FileModel) WriteToStat(stat *syscall.Stat_t) {

	stat.Ino = uint64(file.StIno)
	stat.Mode = uint32(file.FileType) | uint32(file.StMode)

	stat.Uid = uint32(file.StUID)
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

// AdjustNormal 当FileModel是由 WebHDFS 接口获取时，需要调用该接口对部分属性进行转换
func (file *FileModel) AdjustNormal() {

	file.StMtime = util.MsToNs(file.StMtime)

	switch file.HadoopType {
	case HadoopDir:
		file.FileType = TypeDir
		file.StSize = 4096
		file.StNlink = 2
		if file.StAtime == 0 {
			file.StAtime = file.StMtime
		}
	case HadoopFile:
		file.FileType = TypeFile
		file.StNlink = 1
	case HadoopSymlink:
		file.FileType = TypeSymlink
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
	file.StUID = uint(uid)

	fileGroup, err := user.LookupGroup(file.HadoopOwner)
	if err != nil {
		fileGroup, _ = user.LookupGroup("nogroup")
	}
	gid, _ := strconv.Atoi(fileGroup.Gid)
	file.StGid = uint(gid)
}

// ToFuseDirent 将FileModel导出为一个fuse.Dirent的实例中
func (file *FileModel) ToFuseDirent() fuse.Dirent {
	ent := fuse.Dirent{}

	ent.Ino = uint64(file.StIno)
	ent.NameLen = uint32(len(file.Name))
	ent.DirType = uint32(file.StMode)
	ent.Name = file.Name

	return ent
}
