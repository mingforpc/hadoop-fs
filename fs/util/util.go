package util

import (
	"fmt"
	"strconv"
	"strings"
)

// GetParentPath 获取给定路径的父目录
func GetParentPath(path string) string {

	path = strings.TrimRight(path, "/")

	index := strings.LastIndex(path, "/")

	if index > 0 {
		return path[0 : index+1]
	}
	return "/"
}

// GetFileName 获取给定路径的文件名（不包含路径）
func GetFileName(path string) string {
	path = strings.TrimRight(path, "/")

	index := strings.LastIndex(path, "/")

	if index >= 0 {
		return path[index+1:]
	}
	return path
}

// MergePath 合并路径
func MergePath(parent, file string) string {
	var filePath string
	if parent != "" && parent[len(parent)-1] != '/' {
		filePath = parent + "/" + file
	} else {
		filePath = parent + file
	}

	return filePath
}

// ModeToStr 将文件的权限转换成字符串模式，比如:“777”
func ModeToStr(mode uint32) string {

	fmt.Println(strconv.FormatInt(int64(mode), 8))
	mode = mode & 01777
	fmt.Println(strconv.FormatInt(int64(mode), 8))
	return strconv.FormatInt(int64(mode), 8)
}

// NsToMs 纳秒 转 毫秒
func NsToMs(ns int64) int64 {

	return ns / 1000000

}

// MsToNs 毫秒 转 纳秒
func MsToNs(ms int64) int64 {
	return ms * 1000000
}
