package util

import (
	"strconv"
	"strings"
)

func GetParentPath(path string) string {

	path = strings.TrimRight(path, "/")

	index := strings.LastIndex(path, "/")

	if index > 0 {
		return path[0 : index+1]
	} else {
		return "/"
	}
}

func GetFileName(path string) string {
	path = strings.TrimRight(path, "/")

	index := strings.LastIndex(path, "/")

	if index >= 0 {
		return path[index+1:]
	} else {
		return path
	}
}

func MergePath(parent, file string) string {
	var filePath string
	if parent != "" && parent[len(parent)-1] != '/' {
		filePath = parent + "/" + file
	} else {
		filePath = parent + file
	}

	return filePath
}

// 将文件的权限转换成字符串模式，比如:“777”
func ModeToStr(mode uint32) string {
	mode = mode & 0x1777
	return strconv.FormatInt(int64(mode), 8)
}

// 纳秒 转 毫秒
func NsToMs(ns int64) int64 {

	return ns / 1000000

}

// 毫秒 转 纳秒
func MsToNs(ms int64) int64 {
	return ms * 1000000
}
