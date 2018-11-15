# hadoop-fs

利用Fuse和Hadoop WebHDFS REST API 将 HDFS Mount到本地目录的小程序

**目前...只支持列出文件和读取数据，写还没有支持**

## 使用

`go build`之后。

`./hadoop-fs -mp /home/ming/golang/project/src/transfer-fs/test -hadoop_host 192.168.50.254 -hadoop_port 50070`

* `mp` 是 mountpoint，挂载的目录
* `hadoop_host` 是Hadoop的IP
* `hadoop_port` 是WebHDFS REST API的端口

其他可以选项使用`./hadoop-fs --help`查看

