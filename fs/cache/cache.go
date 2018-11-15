package cache

import (
	"sync"
	"time"
)

type notExistFile struct {
	name string
	// timeout in ns
	negativeTime int64
}

// if negative not timeout return true
// else return false
func (file *notExistFile) isNegative() bool {

	if file.negativeTime > time.Now().UnixNano() {
		return true
	}

	return false
}

type NotExistCache struct {
	NegativeTimeout int

	dict map[string]*notExistFile

	lk sync.RWMutex
}

func (cache *NotExistCache) Init() {
	cache.dict = make(map[string]*notExistFile)
}

func (cache *NotExistCache) Insert(filepath string, negativeTimeout int) {

	negativeTime := time.Now().UnixNano() + (int64(negativeTimeout) * 1000000000)

	file := &notExistFile{name: filepath, negativeTime: negativeTime}

	cache.lk.Lock()
	cache.dict[filepath] = file
	cache.lk.Unlock()

}

// if filepath is not exist in cache(so you may test file is exist), return true
// else return false
func (cache *NotExistCache) IsNotExist(filepath string) bool {

	result := false

	var file *notExistFile

	cache.lk.RLock()

	file = cache.dict[filepath]

	if file == nil || !file.isNegative() {
		result = true
	}

	cache.lk.RUnlock()

	if result == true && file != nil {
		cache.delete(filepath)
	}

	return result
}

func (cache *NotExistCache) delete(filepath string) {
	cache.lk.Lock()
	delete(cache.dict, filepath)
	cache.lk.Unlock()
}
