package util

import (
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
