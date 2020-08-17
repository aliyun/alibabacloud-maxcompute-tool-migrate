# Hive迁移至MaxCompute
在Hive迁移至MaxCompute的场景下，MMA实现了Hive的UDTF，通过Hive的分布式能力，实现Hive数据向MaxCompute的高并发传输。

这种迁移方式的优点有：
- 读数据由Hive自身完成，因此可以被Hive读的数据（包括Hive外表），都可以用MMA向MaxCompute迁移，且不存在任何数据格式问题
- 支持增量数据迁移
- 迁移效率高，迁移速率可以随资源分配线性提高

这种迁移方式的前置条件有：
- Hive集群各节点需要具备访问MaxCompute的能力

## 准备工作
### 1. 确认Hive版本
在Hadoop集群的master节点执行 ```hive --version``` 确认Hive版本，根据返回下载对应MMA安装包。

例如：
```$xslt
$ hive --version
Hive 1.0.0
```

此时应选择Github release页面下，mma-hive-1.x-release.tar.gz。


### 2. 确认Hive集群各个节点具备访问MaxCompute的能力

首先确认MaxCompute endpoint，官方参考文档：https://help.aliyun.com/document_detail/34951.html

根据阿里云各Region的部署及网络情况，您可以通过以下三种连接方式访问MaxCompute服务和Tunnel服务：
- 从外网访问MaxCompute服务和Tunnel服务
- 从阿里云经典网络访问MaxCompute服务和Tunnel服务
- 从阿里云VPC网络访问MaxCompute服务和Tunnel服务

以上三种连接方式使用的MaxCompute endpoint有所区别，对于在Aliyun上搭建的Hive集群，或到Aliyun有专线的Hive集群，见上面文档中VPC网络下Region和服务连接对照表。对于其他情况，见外网网络下地域和服务连接对照表。

确认MaxCompute endpoint后，可以在Hive集群各个节点分别执行 ```curl "MaxCompute endpoint"```，如果命令立刻返回，
说明可以访问MaxCompute。

例如：

```$xslt
$ curl http://service.cn-hangzhou.maxcompute.aliyun-inc.com/api
<?xml version="1.0" encoding="UTF-8"?>
<Error>
	<Code>NoSuchObject</Code>
	<Message><![CDATA[Unknown http request location: /]]></Message>
	<RequestId>5E748282CA7BCB32D7651D20</RequestId>
	<HostId>localhost</HostId>
</Error>
```

即表示该节点可以访问MaxCompute。

## 配置
首先解压MMA安装包。之后执行以下命令，运行配置引导脚本，完成配置：
```$xslt
$ mma/bin/configure
```

配置过程中需要提供以下Hive参数：

|参数名      |含义        |示例        |
|:----------|:----------|:----------|
|Hive metastore URI(s)|见hive-site.xml中"hive.metastore.uris"|thrift://hostname:9083|
|Hive JDBC连接串|通过beeline使用Hive时输入的JDBC连接串, 前缀为jdbc:hive2|jdbc:hive2://hostname:10000/default|
|Hive JDBC连接用户名|通常通过beeline使用Hive时输入的JDBC连接用户名, 默认值为Hive|Hive|
|Hive JDBC连接密码|通常通过beeline使用Hive时输入的JDBC连接密码, 默认值为空||

配置过程中需要提供以下MaxCompute参数：

|参数名      |含义        |示例        |
|:----------|:----------|:----------|
|MaxCompute endpoint|上文中获取的MaxCompute endpoint|http://service.cn-hangzhou.maxcompute.aliyun.com/api|
|MaxCompute project名|建议配置为目标MaxCompute project, 规避权限问题||
|阿里云accesskey id|详见: https://help.aliyun.com/document_detail/27803.html||
|阿里云accesskey secret|详见: https://help.aliyun.com/document_detail/27803.html||

此外，配置过程中还需要将某些文件上传至HDFS，并在beeline中创建MMA需要的Hive永久函数。MMA配置引导脚本会自动生成需要执行的命令，直接复制粘贴到安装有hdfs命令与beeline的服务器上执行即可。命令示例如下：


上传文件至HDFS：
```$xslt
$ hdfs dfs -put -f /path/to/mma/conf/odps_config.ini hdfs:///tmp/
$ hdfs dfs -put -f /path/to/mma/lib/data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///tmp/
```

创建函数：
```$xslt
0: jdbc:hive2://127.0.0.1:10000/default> CREATE FUNCTION odps_data_dump_multi as 'com.aliyun.odps.datacarrier.transfer.OdpsDataTransferUDTF' USING JAR 'hdfs:///tmp/data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar';
```

## 快速开始
MMA提供了quickstart脚本帮助用户快速熟悉MMA的使用方式。完成配置后，可以选择一张表进行迁移。quickstart脚本需要Hive库名，Hive表名，MaxCompute project名以及MaxCompute表名。需要注意的是MMA会自动创建MaxCompute中的目标表，如果目标表已经存在，可能会发生数据类型冲突等问题。

执行以下命令运行quickstart脚本：
```$xslt
$ mma/bin/quickstart hive_source_db hive_source_table mc_dest_project mc_dest_table
```

## 最佳实践
### 存量数据迁移
在存量数据迁移的场景下，我们可以通过以下步骤完成数据迁移：
1. 确认待迁移的表：由于开发和生产过程中，会产生很多已经废弃不用的表或忘记清理的临时表。这些表往往不再具有价值，因此无需进行迁移。忽略这些表会大大节省数据迁移过程中的时间/计算资源开销，同时也是一次很好的业务梳理机会。
1. 启动MmaServer，见[启动MMA server](#StartMmaServer)
1. 生成迁移任务配置文件，见[生成任务配置](#GenerateJobConfig)
1. 提交迁移任务，与[提交迁移任务](#SubmitJob)
1. 反复执行1, 2, 3三步，通过MMA client向MMA server提交所有待迁移表
1. 等待迁移任务完成（可以另开一个terminal追踪进度，与1, 2, 3步不冲突），见[查看进度](#GetProgress)
1. 处理失败的任务并重启任务，见[查看迁移任务列表](#ListJobs)与[失败处理](#HandleFailures)

### 增量数据迁移
当存量数据通过MMA进行迁移之后，MMA支持对新增分区或最近被修改的分区进行增量数据进行迁移。当您确认新增的分区已经处于可迁移的状态（不会有新数据进入该分区），可以通过直接重新提交迁移任务，让MMA获取新增的分区，并进行迁移。迁移步骤如下：
1. 完成存量数据迁移
1. 确认源表不再有修改，可以迁移
1. 重启迁移任务，见[提交迁移任务](#SubmitJob)
1. 等待迁移任务完成，见[查看进度](#GetProgress)

## 使用说明
### <a name="StartMmaServer"></a>启动MMA server
执行以下命令启动MMA server，MMA server进程在迁移期间应当一直保持运行。若MMA server因为各种原因中断了运行，直接执行以上命令重启即可。MMA server进程在一台服务器最多只能存在一个。

```$xslt
$ nohup /path/to/mma/bin/mma-server >/dev/null 2>&1 &
```

### <a name="GenerateJobConfig"></a>生成任务配置
-  表级别任务配置
表级别的迁移是MMA主要支持的场景，因此我们提供了generate-config来更方便地生成任务配置文件。首先组织临时文件table_mapping.txt，模版如下：
```$xslt
# The following example represents a migration job. The source table is 'source_table' in Hive
# database 'source_db' and the destination table is 'dest_table' in MaxCompute project 'dest_pjt'

test_db.test_table:test_project.test_table
```
table_mapping.txt中的每一行表示一张Hive表到MaxCompute表的映射。之后执行以下命令直接生成table_mapping.txt文件中包含的迁移任务配置。
```$xslt
$ /path/to/mma/bin/generate-config --to_migration_config --table_mapping table_mapping.txt
```
执行完成后，当前目录下将会生成MMA迁移任务的配置文件mma_migration_config.json。

-  库级别任务配置
MMA支持迁移Hive中的一个库至MaxCompute，此时任务迁移文件需要手动编写，模版如下：
```$xslt
{
  "user": "Jerry",
  "databaseMigrationConfigs": [
    {
      "sourceDatabaseName": "test_db",
      "destProjectName": "test_project"
    }
  ]
}
```
修改模版中"databaseMigrationConfigs"下的"sourceDatabaseName"即可改变源库名；修改"destProjectName"即可改变目标MaxCompute project名。

### <a name="SubmitJob"></a>提交任务
执行以下命令，向MMA server提交迁移任务：
```$xslt
$ /path/to/mma/bin/mma-client --start mma_migration_config.json
```
任务提交成功时，MMA client会打印 ```Job submitted``` 并结束进程，返回值为0：

### <a name="GetProgress"></a>查看进度
执行以下命令，查看目前MMA所有迁移任务的进度：
```$xslt
$ /path/to/mma/bin/mma-client --wait_all
```

MMA将会打印当前所有迁移任务的进度条，当所有任务完成之后结束进程，返回值为0。

### <a name="ListJobs"></a>查看迁移任务列表
执行以下命令，查看所有迁移任务列表：
```$xslt
$ /path/bin/mma-client --list all
```
将all替换为PENDING，RUNNING，SUCCEEDED，或FAILED可以查看该状态下迁移任务的列表。

### <a name="RemoveJob"></a>删除迁移任务
执行以下命令，可以删除状态为SUCCEEDED或FAILED的迁移任务。
```$xslt
$ /path/to/mma/bin/mma-client --remove hive_source_db.hive_source_table
```

## <a name="HandleFailures"></a>失败处理
TODO
