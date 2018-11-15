package config

import (
	"flag"
	"fmt"
	"os"
)

type HadoopConfig struct {
	IsSSL bool
	Host  string
	Port  int

	Username string

	Delegation string
}

type Config struct {
	Mountpoint  string
	Attrtimeout float64

	Debug                bool // 是否是debug模式
	NotExistCacheTimeout int  // 文件不存在会缓存的时间，单位秒

	Hadoop HadoopConfig
}

func ParseFromCmd() Config {

	config := Config{}

	flag.StringVar(&config.Mountpoint, "mp", "", "mountpoint")
	flag.Float64Var(&config.Attrtimeout, "attr_timeout", 10, "file attr timeout")

	flag.BoolVar(&config.Hadoop.IsSSL, "hadoop_ssl", false, "If Hadoop WebHDFS REST API use HTTP?")
	flag.StringVar(&config.Hadoop.Host, "hadoop_host", "", "Hadoop WebHDFS REST API hostname or IP")
	flag.IntVar(&config.Hadoop.Port, "hadoop_port", -1, "Hadoop WebHDFS REST API port")
	flag.StringVar(&config.Hadoop.Username, "hadoop_username", "", "Hadoop WebHDFS REST API username")
	flag.StringVar(&config.Hadoop.Delegation, "hadoop_delegation", "", "Hadoop WebHDFS REST API delegation")
	flag.BoolVar(&config.Debug, "debug", false, "Debug Mode")
	flag.IntVar(&config.NotExistCacheTimeout, "not_exist_cache", 200, "How long for not exist file cache, default is 200s")

	// mountpoint是必填的
	if config.Mountpoint == "" {
		fmt.Println("Please input mountpoint!")
		os.Exit(-1)
	}

	// 检查mountpoint是否存在，而且是否是文件夹
	stat, err := os.Stat(config.Mountpoint)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	if !stat.IsDir() {
		fmt.Println("Mountpoint is not a directory!")
		os.Exit(-1)
	}

	// host和port是必填的
	if config.Hadoop.Host == "" {
		fmt.Println("Please input Hadoop WebHDFS REST API hostname or IP!")
		os.Exit(-1)

	}
	if config.Hadoop.Port < 0 {
		fmt.Println("Please input Hadoop WebHDFS REST API port!")
		os.Exit(-1)
	}

	return config
}
