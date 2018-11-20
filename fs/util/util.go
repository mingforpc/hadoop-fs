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
	mode = mode & 0x0777
	return strconv.FormatInt(int64(mode), 8)
}
