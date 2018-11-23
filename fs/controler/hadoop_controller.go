package controler

import (
	"bytes"
	"encoding/json"
	"fmt"
	herr "hadoop-fs/fs/controler/hadoop_error"
	"hadoop-fs/fs/logger"
	"hadoop-fs/fs/model"
	"net/http"
	"strconv"
)

const (
	LISTSTATUS_BATCH = "LISTSTATUS_BATCH"
	GETFILESTATUS    = "GETFILESTATUS"
	READ             = "OPEN"
	MKDIRS           = "MKDIRS"
	CREATE           = "CREATE"
	SETTIMES         = "SETTIMES"
	APPEND           = "APPEND"
	TRUNCATE         = "TRUNCATE"
	DELETE           = "DELETE"
)

var _default_buffersize = 4096
var _default_length = 4096

type HadoopController struct {
	isSSL bool
	host  string
	port  int

	username string

	httpPrefix string

	inited bool
}

func (hadoop *HadoopController) Init(ssl bool, host string, port int, username string) {

	hadoop.isSSL = ssl
	if ssl {
		hadoop.httpPrefix = "https"
	} else {
		hadoop.httpPrefix = "http"
	}

	hadoop.host = host
	hadoop.port = port
	hadoop.username = username

	hadoop.inited = true

}

func (hadoop *HadoopController) urlJoin(path, op string) string {
	var url string
	if hadoop.username != "" {
		url = fmt.Sprintf("%s://%s:%d/webhdfs/v1%s?user.name=%s&op=%s", hadoop.httpPrefix, hadoop.host, hadoop.port, path, hadoop.username, op)
	} else {
		url = fmt.Sprintf("%s://%s:%d/webhdfs/v1%s?op=%s", hadoop.httpPrefix, hadoop.host, hadoop.port, path, op)
	}

	return url
}

// 列出目录下的文件
func (hadoop *HadoopController) List(path, startAfter string) (fileList []model.FileModel, remain int, err error) {

	defer func() {
		if err := recover(); err != nil {
			logger.Error.Println(err)
		}
	}()

	url := hadoop.urlJoin(path, LISTSTATUS_BATCH)

	if startAfter != "" {
		url = urlAddParam(url, "startAfter", startAfter)
	}

	resp, err := http.Get(url)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)

	buf.ReadFrom(resp.Body)

	if resp.StatusCode != 200 {
		exception := HadoopException{}
		err = json.Unmarshal(buf.Bytes(), &exception)
		if err != nil {
			panic(err)
		}
		switch resp.StatusCode {
		case 404:
			panic(herr.NO_FOUND)
		default:
			panic(exception)
		}
	}

	logger.Trace.Println(buf.String())

	statusBatch := ListStatusBatch{}
	err = json.Unmarshal(buf.Bytes(), &statusBatch)

	if err != nil {
		panic(err)
	}

	fileList = statusBatch.GetFiles()
	remain = statusBatch.PemainingEntries

	return
}

func recoverError(exception *error) {
	if err := recover(); err != nil {
		*exception = err.(error)
		logger.Error.Println(err)
	}
}

// 获取文件信息
func (hadoop *HadoopController) GetFileStatus(filePath string) (file model.FileModel, err error) {

	defer recoverError(&err)

	url := hadoop.urlJoin(filePath, GETFILESTATUS)

	resp, err := http.Get(url)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)

	buf.ReadFrom(resp.Body)

	if resp.StatusCode != 200 {
		exception := HadoopException{}
		err = json.Unmarshal(buf.Bytes(), &exception)
		if err != nil {
			panic(err)
		}
		switch resp.StatusCode {
		case 404:
			panic(herr.NO_FOUND)
		default:
			panic(exception)
		}
	}

	fileStatus := GetFileStatus{}
	err = json.Unmarshal(buf.Bytes(), &fileStatus)

	if err != nil {
		panic(err)
	}

	file = fileStatus.GetFile()

	return file, err
}

// 读取文件内容
func (hadoop *HadoopController) Read(filePath string, offset uint64, length uint32, buffersize int) (content []byte, err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filePath, READ)

	url = urlAddParam(url, "offset", strconv.FormatInt(int64(offset), 10))

	if length <= 0 {
		length = uint32(_default_length)
	}
	if buffersize <= 0 {
		buffersize = _default_buffersize
	}
	url = urlAddParam(url, "length", strconv.FormatInt(int64(length), 10))
	url = urlAddParam(url, "buffersize", strconv.Itoa(buffersize))

	resp, err := http.Get(url)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)

	buf.ReadFrom(resp.Body)

	if resp.StatusCode != 200 {
		exception := HadoopException{}
		err = json.Unmarshal(buf.Bytes(), &exception)
		if err != nil {
			panic(err)
		}
		switch resp.StatusCode {
		case 404:
			panic(herr.NO_FOUND)
		case 403:
			panic(herr.EOF)
		default:
			panic(exception)
		}

	}

	content = buf.Bytes()

	return content, err
}

// 创建目录
func (hadoop *HadoopController) MakeDir(pathname, permission string) (result bool, err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(pathname, MKDIRS)

	if permission != "" {
		url = urlAddParam(url, "permission", permission)
	}

	req, err := http.NewRequest("PUT", url, nil)

	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)
	buf.ReadFrom(resp.Body)

	if resp.StatusCode != 200 {
		exception := HadoopException{}
		err = json.Unmarshal(buf.Bytes(), &exception)
		if err != nil {
			panic(err)
		}
		switch resp.StatusCode {
		case 403:
			panic(herr.EACCES)
		default:
			panic(exception)
		}
	}

	booleanRes := BooleanResp{}
	err = json.Unmarshal(buf.Bytes(), &booleanRes)

	if err != nil {
		panic(err)
	}

	return booleanRes.Boolean, err
}

// 创建文件
func (hadoop *HadoopController) Create(filepath, permission string) (err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, CREATE)

	if permission != "" {
		url = urlAddParam(url, "permission", permission)
	}
	url = urlAddParam(url, "overwrite", "false")

	req, err := http.NewRequest("PUT", url, nil)

	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)
	buf.ReadFrom(resp.Body)

	if resp.StatusCode != 201 {
		exception := HadoopException{}
		err = json.Unmarshal(buf.Bytes(), &exception)
		if err != nil {
			panic(err)
		}
		switch exception.Error() {
		case "AccessControlException":
			panic(herr.EACCES)
		case "FileAlreadyExistsException":
			panic(herr.EEXIST)
		default:
			panic(exception)
		}
	}

	return err
}

// 设置文件Mtime和Atime，-1表示不变
func (hadoop *HadoopController) ModificationTime(filepath string, mtime, atime int64) (err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, SETTIMES)

	url = urlAddParam(url, "modificationtime", strconv.FormatInt(mtime, 10))
	url = urlAddParam(url, "accesstime", strconv.FormatInt(atime, 10))

	req, err := http.NewRequest("PUT", url, nil)

	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)
	buf.ReadFrom(resp.Body)

	if resp.StatusCode != 200 {
		exception := HadoopException{}
		err = json.Unmarshal(buf.Bytes(), &exception)
		if err != nil {
			panic(err)
		}
		switch exception.Error() {
		case "AccessControlException":
			panic(herr.EACCES)
		default:
			panic(exception)
		}
	}

	return err
}

// 追加文件内容
func (hadoop *HadoopController) AppendFile(filepath string, content []byte) (err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, APPEND)

	contentBuf := bytes.NewBuffer(content)

	resp, err := http.Post(url, "application/octet-stream", contentBuf)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)
	buf.ReadFrom(resp.Body)

	if resp.StatusCode != 200 {
		exception := HadoopException{}
		err = json.Unmarshal(buf.Bytes(), &exception)
		if err != nil {
			panic(err)
		}
		switch exception.Error() {
		case "AccessControlException":
			panic(herr.EACCES)
		default:
			panic(exception)
		}
	}

	return err
}

// Truncate 文件
func (hadoop *HadoopController) TruncateFile(filepath string, newlength int64) (result bool, err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, TRUNCATE)
	url = urlAddParam(url, "newlength", strconv.FormatInt(newlength, 10))

	resp, err := http.Post(url, "application/json", nil)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)
	buf.ReadFrom(resp.Body)

	if resp.StatusCode != 200 {
		exception := HadoopException{}
		err = json.Unmarshal(buf.Bytes(), &exception)

		if err != nil {
			panic(err)
		}
		switch resp.StatusCode {
		case 404:
			panic(herr.EEXIST)
		case 403:
			panic(herr.EACCES)
		default:
			panic(exception)
		}
	}

	booleanRes := BooleanResp{}
	err = json.Unmarshal(buf.Bytes(), &booleanRes)

	if err != nil {
		panic(err)
	}

	return booleanRes.Boolean, err
}

// 删除文件或者目录
func (hadoop *HadoopController) Delete(filepath string) (result bool, err error) {

	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, DELETE)

	req, err := http.NewRequest("DELETE", url, nil)

	if err != nil {
		panic(err)
	}

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		panic(err)
	}

	defer resp.Body.Close()

	buf := bytes.NewBuffer(nil)
	buf.ReadFrom(resp.Body)

	if resp.StatusCode != 200 {
		exception := HadoopException{}
		err = json.Unmarshal(buf.Bytes(), &exception)

		if err != nil {
			panic(err)
		}
		switch resp.StatusCode {
		case 404:
			panic(herr.EEXIST)
		case 403:
			panic(herr.EACCES)
		default:
			panic(exception)
		}
	}

	booleanRes := BooleanResp{}
	err = json.Unmarshal(buf.Bytes(), &booleanRes)

	if err != nil {
		panic(err)
	}

	return booleanRes.Boolean, err
}

func urlAddParam(url, name, val string) string {
	return url + "&" + name + "=" + val
}
