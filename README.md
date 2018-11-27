# Youseed磁力爬虫入库程序 #

此程序使用Java编写，负责将rabbitMQ消息队列中的数据保存至数据库或者搜索引擎。

*此程序仅用作技术学习和研究*

# 功能 #

读取消息队列，将爬虫抓取到的数据保存至：

- Youseed Mongodb数据库；
- Youseed Elasticsearch搜索引擎；
- “纸上烤鱼磁力搜索引擎”数据库


**注意**：此爬虫程序主要负责保存数据，需要配合“dht_spider.py”，或者“dht_spider_zsky.py”爬虫程序使用。

# 程序特点 #

1. 兼容性：支持Mongodb、Mysql和Elasticsearch搜索引擎；
2. 实时和定时：支持Elasticsearch中新资源的实时索引，支持旧资源的定时更新；


# 硬件要求 #

- 内存：约200M

# 软件要求 #

需要安装以下软件：

- jdk运行环境

# 安装（以centos7为例） #

## 安装JDK ##

    yum install java-1.8.0-openjdk.x86_64

## 下载程序 ##

将编译好的jar包`spider-saver-public-1.0.0.jar`和配置文件`youseed-spider-saver.yml`下载至本地。

## 修改配置 ##

编辑文件`youseed-spider-saver.yml`，修改连接配置：

    #MongoDB连接配置
    mongo: 
      url: 127.0.0.1
      port: 27017
      db: seed
      admindb: 
      user: 
      psw: 
    
    #ES搜索引擎连接配置 
    es:
      url: 127.0.0.1
      port: 9300
    
    #mysql连接配置（for 纸上烤鱼） <------------------纸上烤鱼程序修改这个连接
    mysql:
      url: jdbc:mysql://localhost:3306/zsky?useUnicode=true&characterEncoding=utf-8&serverTimezone=GMT%2B8
      user: root
      psw: 