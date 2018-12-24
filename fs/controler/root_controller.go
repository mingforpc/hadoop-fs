package controler

import (
	"hadoop-fs/fs/model"
	"os/user"
	"strconv"
	"syscall"

	"github.com/mingforpc/fuse-go/fuse"
)

var ROOT RootController

func init() {
	ROOT = RootController{}
}

type RootController struct {
	rootFile *model.FileModel
}

func (rc *RootController) GetRoot(req fuse.Req) model.FileModel {
	if rc.rootFile != nil {
		return *rc.rootFile
	} else {
		user, _ := user.Current()

		uid, _ := strconv.Atoi(user.Uid)
		gid, _ := strconv.Atoi(user.Gid)

		rc.rootFile = &model.FileModel{}
		rc.rootFile.StMode = syscall.S_IFDIR | uint(0777)

		rc.rootFile.StUid = uint(uid)
		rc.rootFile.StGid = uint(gid)

		rc.rootFile.StNlink = 2

		rc.rootFile.StSize = 4096

		config := req.GetFuseConfig()

		rc.rootFile.StAtime = config.FuseStartTime
		rc.rootFile.StMtime = config.FuseStartTime
		rc.rootFile.StCtime = config.FuseStartTime

		return *rc.rootFile
	}
}
