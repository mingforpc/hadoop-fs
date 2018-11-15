package cache

import (
	"sync"
)

// 用来保存文件nodeid和对应path的map结构
type FusePathManager struct {
	pathDict map[uint64]string

	lk sync.RWMutex
}

func (fp *FusePathManager) Init() {
	fp.pathDict = make(map[uint64]string)

	fp.pathDict[1] = "/"
}

func (fp *FusePathManager) Insert(nodeid uint64, path string) {
	fp.lk.Lock()
	fp.pathDict[nodeid] = path
	fp.lk.Unlock()
}

func (fp *FusePathManager) Get(nodeid uint64) string {
	fp.lk.RLock()
	path := fp.pathDict[nodeid]
	fp.lk.RUnlock()
	return path
}

func (fp *FusePathManager) Delete(nodeid uint64) {
	fp.lk.Lock()
	delete(fp.pathDict, nodeid)
	fp.lk.Unlock()
}
