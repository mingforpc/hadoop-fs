package controler

import (
	"hadoop-fs/fs/model"
	"os/user"
	"strconv"
	"syscall"

	"github.com/mingforpc/fuse-go/fuse"
)

// ROOT 根目录的控制类的全局变量
var ROOT RootController

func init() {
	ROOT = RootController{}
}

// RootController 根目录的控制类，主要与根目录部分操作相关联
type RootController struct {
	rootFile *model.FileModel
}

// GetRoot 获取根目录的文件信息
func (rc *RootController) GetRoot(req fuse.Req) model.FileModel {
	if rc.rootFile != nil {
		return *rc.rootFile
	}
	user, _ := user.Current()

	uid, _ := strconv.Atoi(user.Uid)
	gid, _ := strconv.Atoi(user.Gid)

	rc.rootFile = &model.FileModel{}
	rc.rootFile.StMode = syscall.S_IFDIR | uint(0777)

	rc.rootFile.StUID = uint(uid)
	rc.rootFile.StGid = uint(gid)

	rc.rootFile.StNlink = 2

	rc.rootFile.StSize = 4096

	config := req.GetFuseConfig()

	rc.rootFile.StAtime = config.FuseStartTime
	rc.rootFile.StMtime = config.FuseStartTime
	rc.rootFile.StCtime = config.FuseStartTime

	return *rc.rootFile

}
