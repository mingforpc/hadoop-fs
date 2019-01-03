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

// op code
const (
	opListStatusBatch = "LISTSTATUS_BATCH"
	opGetFileStatus   = "GETFILESTATUS"
	opRead            = "OPEN"
	opMkDir           = "MKDIRS"
	opCreate          = "CREATE"
	opSetTimes        = "SETTIMES"
	opAppend          = "APPEND"
	opTruncate        = "TRUNCATE"
	opDelete          = "DELETE"
	opSetPermission   = "SETPERMISSION"
	opRename          = "RENAME"
	opCreateSymlink   = "CREATESYMLINK"
	opSetXattr        = "SETXATTR"
	opGetXattr        = "GETXATTRS"
	opRemoveXattr     = "REMOVEXATTR"
)

var defaultBufferSize = 4096
var defaultLength = 4096

// HadoopController 与Hadoop WebHDFS 交互的控制类
type HadoopController struct {
	isSSL bool
	host  string
	port  int

	username string

	httpPrefix string

	inited bool
}

// Init 初始化函数
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

// List 列出目录下的文件
func (hadoop *HadoopController) List(path, startAfter string) (fileList []model.FileModel, remain int, err error) {

	defer func() {
		if err := recover(); err != nil {
			logger.Error.Println(err)
		}
	}()

	url := hadoop.urlJoin(path, opListStatusBatch)

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
			panic(herr.ErrNoFound)
		default:
			panic(exception)
		}
	}

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

// GetFileStatus 获取文件信息
func (hadoop *HadoopController) GetFileStatus(filePath string) (file model.FileModel, err error) {

	defer recoverError(&err)

	url := hadoop.urlJoin(filePath, opGetFileStatus)

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
			panic(herr.ErrNoFound)
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

	url := hadoop.urlJoin(filePath, opRead)

	url = urlAddParam(url, "offset", strconv.FormatInt(int64(offset), 10))

	if length <= 0 {
		length = uint32(defaultLength)
	}
	if buffersize <= 0 {
		buffersize = defaultBufferSize
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
			panic(herr.ErrNoFound)
		case 403:
			panic(herr.ErrEOF)
		default:
			panic(exception)
		}

	}

	content = buf.Bytes()

	return content, err
}

// MakeDir 创建目录
func (hadoop *HadoopController) MakeDir(pathname, permission string) (result bool, err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(pathname, opMkDir)

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
			panic(herr.ErrAccess)
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

// Create 创建文件
func (hadoop *HadoopController) Create(filepath, permission string) (err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, opCreate)

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
			panic(herr.ErrAccess)
		case "FileAlreadyExistsException":
			panic(herr.ErrExist)
		default:
			panic(exception)
		}
	}

	return err
}

// ModificationTime 设置文件Mtime和Atime，-1表示不变
func (hadoop *HadoopController) ModificationTime(filepath string, mtime, atime int64) (err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, opSetTimes)

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
			panic(herr.ErrAccess)
		default:
			panic(exception)
		}
	}

	return err
}

// AppendFile 追加文件内容
func (hadoop *HadoopController) AppendFile(filepath string, content []byte) (err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, opAppend)

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
			panic(herr.ErrAccess)
		default:
			panic(exception)
		}
	}

	return err
}

// TruncateFile Truncate 文件
func (hadoop *HadoopController) TruncateFile(filepath string, newlength int64) (result bool, err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, opTruncate)
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
			panic(herr.ErrExist)
		case 403:
			panic(herr.ErrAccess)
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

// Delete 删除文件或者目录
func (hadoop *HadoopController) Delete(filepath string) (result bool, err error) {

	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, opDelete)

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
			panic(herr.ErrExist)
		case 403:
			panic(herr.ErrAccess)
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

// SetPermission 设置文件权限
func (hadoop *HadoopController) SetPermission(filepath, permission string) (err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, opSetPermission)
	url = urlAddParam(url, "permission", permission)

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
		case 404:
			panic(herr.ErrExist)
		case 403:
			panic(herr.ErrAccess)
		default:
			panic(exception)
		}
	}

	return err
}

// Rename 文件重命名
func (hadoop *HadoopController) Rename(src, dest string) (result bool, err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(src, opRename)
	url = urlAddParam(url, "destination", dest)

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
		case 404:
			panic(herr.ErrExist)
		case 403:
			panic(herr.ErrAccess)
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

// CreateSymlink 创建软连接
func (hadoop *HadoopController) CreateSymlink(src, link string) (err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(src, opCreateSymlink)
	url = urlAddParam(url, "destination", link)

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
		case 404:
			panic(herr.ErrExist)
		case 403:
			panic(herr.ErrAccess)
		default:
			panic(exception)
		}
	}

	return err
}

// Setxattr setxattr
func (hadoop *HadoopController) Setxattr(filepath, name, value, flag string) (err error) {
	defer recoverError(&err)

	url := hadoop.urlJoin(filepath, opSetXattr)
	url = urlAddParam(url, "xattr.name", name)
	url = urlAddParam(url, "xattr.value", value)
	url = urlAddParam(url, "flag", flag)

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
		case 400:
			panic(herr.ErrNotsup)
		case 404:
			panic(herr.ErrNoFound)
		case 403:
			// xattr已经存在
			panic(herr.ErrExist)
		default:
			panic(exception)
		}
	}

	return err
}

// Getxattr getxattr
func (hadoop *HadoopController) Getxattr(filepath, name string) (value string, err error) {
	recoverError(&err)

	url := hadoop.urlJoin(filepath, opGetXattr)
	url = urlAddParam(url, "xattr.name", name)
	url = urlAddParam(url, "encoding", "text")

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
			panic(herr.ErrNoFound)
		default:
			panic(exception)
		}
	}

	attrs := XattrsResp{}
	err = json.Unmarshal(buf.Bytes(), &attrs)

	if err != nil {
		panic(err)
	}

	attr := attrs.Xattrs[0]
	value = attr.Value

	return value, err
}

// Listxattr lisstxattr
func (hadoop *HadoopController) Listxattr(filepath string) (attrs []Xattr, err error) {
	recoverError(&err)

	url := hadoop.urlJoin(filepath, opGetXattr)
	url = urlAddParam(url, "encoding", "text")

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
			panic(herr.ErrNoFound)
		default:
			panic(exception)
		}
	}

	attrsresp := XattrsResp{}
	err = json.Unmarshal(buf.Bytes(), &attrsresp)

	if err != nil {
		panic(err)
	}

	attrs = attrsresp.Xattrs

	return attrs, err
}

// Removexattr removexattr
func (hadoop *HadoopController) Removexattr(filepath, name string) (err error) {
	recoverError(&err)

	url := hadoop.urlJoin(filepath, opRemoveXattr)
	url = urlAddParam(url, "xattr.name", name)

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
		case 400:
			panic(herr.ErrNotsup)
		case 403:
			panic(herr.ErrNoAttr)
		case 404:
			panic(herr.ErrNoFound)
		default:
			panic(exception)
		}
	}

	return err
}

func urlAddParam(url, name, val string) string {
	return url + "&" + name + "=" + val
}
