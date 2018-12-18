# hadoop-fs

利用Fuse和Hadoop WebHDFS REST API 将 HDFS Mount到本地目录的小程序

**目前支持文件的主要操作...读写,删除文件(非文件夹), 删除文件夹, 修改文件权限, 重命名, xattr等**

**软连接功能，看起来HDFS不支持[https://issues.apache.org/jira/browse/HDFS-4559](https://issues.apache.org/jira/browse/HDFS-4559)**

## 待实现与优化

* 优化错误返回
* `Access()`的实现
* `chown`和`chgrp`的支持要想想怎么做

## 使用

`go build`之后。

`./hadoop-fs -mp /home/ming/golang/project/src/transfer-fs/test -hadoop_host 192.168.50.254 -hadoop_port 50070`

* `mp` 是 mountpoint，挂载的目录
* `hadoop_host` 是Hadoop的IP
* `hadoop_port` 是WebHDFS REST API的端口
* 如果要执行写操作，一定要设置Hadoop的user，不然会返回没权限

其他可以选项使用`./hadoop-fs --help`查看

## 退出

1. 直接 `kill {pid}`
2. 通过 `fusermount -u {mountpoint}`退出，如果显示busy，则使用`fusermount -uz {mountpoint}`
