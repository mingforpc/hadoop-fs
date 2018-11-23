# hadoop-fs

利用Fuse和Hadoop WebHDFS REST API 将 HDFS Mount到本地目录的小程序

**目前...仅支持读写，  删除文件，创建软连接，重命名，修改文件权限，Xattr等都未实现**

## 使用

`go build`之后。

`./hadoop-fs -mp /home/ming/golang/project/src/transfer-fs/test -hadoop_host 192.168.50.254 -hadoop_port 50070`

* `mp` 是 mountpoint，挂载的目录
* `hadoop_host` 是Hadoop的IP
* `hadoop_port` 是WebHDFS REST API的端口

其他可以选项使用`./hadoop-fs --help`查看

