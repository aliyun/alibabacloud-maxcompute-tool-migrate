# Hive迁移至MaxCompute
在Hive迁移至MaxCompute的场景下，MMA实现了Hive的UDTF，通过Hive的分布式能力，实现Hive数据向MaxCompute的高并发传输。

这种迁移方式的优点有：
- 读数据由Hive自身完成，因此可以被Hive读的数据（包括Hive外表），都可以用MMA向MaxCompute迁移，且不存在任何数据格式问题
- 支持增量数据迁移
- 迁移效率高，迁移速率可以随资源分配线性提高

这种迁移方式的前置条件有：
- Hive集群各节点需要具备访问MaxCompute的能力

## <a name="Architecture"></a>架构与原理
当用户通过MMA client向MMA server提交一个迁移Job后，MMA首先会将该Job的配置记录在元数据中，并初始化其状态为PENDING。

随后，MMA调度器将会把这个Job状态置为RUNNING，向Hive请求这张表的元数据，并开始调度执行。这个Job在MMA中会被拆分为若干
个Task，每一个Task负责表中的一部分数据。每一个Task将会包含如下图所示的，由若干个Action组成的DAG：
```$xslt
            CreateTable (在MC中创建表)
                 |
            AddPartition (在MC中添加分区，仅限分区表)
                 |
            DataTransfer (数据传输)
         +-------+-------+
         |               |
Source Verification   Dest Verification
         |               |
         +-------+-------+
                 |
      Compare Verification Result (对比验证结果)
```

上图中数据传输的原理是利用Hive的分布式计算能力，实现了一个Hive UDTF，在Hive UDTF
中实现了上传数据至MaxCompute的逻辑，并将一个数据迁移任务转化为一个或多个形如：
```$xslt
SELECT UDTF(*) FROM hive_db.hive_table;
```
的Hive SQL。在执行上述Hive SQL时，数据将被Hive读出并传入UDTF，UDTF会通过MaxCompute的Tunnel SDK将数据写入
MaxCompute。

当某一个Task的所有Action执行成功后，MMA会将这个Task负责的部分数据的迁移状态置为SUCCEEDED。当该Job对应的所有Task都成功
后，这张表的迁移结束。

当某一个Task的某一个Action执行失败，MMA会将这个Task负责的部分数据的迁移状态置为FAILED，并生成另一个Task负责这部分数据，
直到成功或达到重试次数上限。

当表中数据发生变化时（新增数据，新增分区，或已有分区数据变化），可以重新提交迁移任务，此时MMA会重新扫描Hive中元数据，
发现数据变化，并迁移发生变化的表或分区。

## <a name="Preparation"></a>准备工作
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

### 3. MaxCompute配置

使用MMA前，需要确认MaxCompute project已经按照[文档](https://help.aliyun.com/document_detail/159541.html?spm=a2c4g.11186623.6.639.7336134dNbODrx)配置了2.0数据类型版本

## <a name="Configuration"></a>配置
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
0: jdbc:hive2://127.0.0.1:10000/default> CREATE FUNCTION odps_data_dump_multi as 'com.aliyun.odps.mma.io.McDataTransmissionUDTF' USING JAR 'hdfs:///tmp/data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar';
```

### 进度推送
MMA支持向钉钉群推送进度信息。目前支持summary，迁移成功以及迁移失败三种类型的事件。使用本功能前需要创建一个钉钉群，并获取
钉钉群自定义机器人的webhook url，方法可以参考[文档](https://ding-doc.dingtalk.com/document#/isv-dev-guide/custom-robot-development)。钉钉机器人安全配置关键字可以配置"succeeded"，"failed"，以及"Summary"，大小写敏感。


之后，在MMA server配置文件的根Json中添加以下配置，用真实的webhook url替换```${webhookurl}```，并重启MMA server。

```$xslt
"eventConfig": {
  "eventSenderConfigs": [
    {
      "type": "DingTalk",
      "webhookUrl": "${webhook_url}"
    }
  ],
}
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
1. 重启迁移任务，此时MMA会自动发现新分区和有修改的分区并进行迁移，见[提交迁移任务](#SubmitJob)
1. 等待迁移任务完成，见[查看进度](#GetProgress)
1. 处理失败的任务并重启任务，见[查看迁移任务列表](#ListJobs)与[失败处理](#HandleFailures)

## MMA 命令行工具
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
test_db_2.test_table:test_project_2.test_table
test_db.test_table_2:test_project_2.test_table_2
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

MMA支持通过WebUI查看目前正在运行的迁移任务，见[Web UI](#WebUI)


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

## <a name="WebUI"></a>MMA Web UI
为了带给用户更好的体验，MMA支持了Web UI。目前Web UI主要用于查看任务的状态，进度，以及有助于错误排查的各种debug信息。

Web UI运行在MMA server所在服务器的18888端口，可以通过`http://${hostname}:18888`地址进行访问。


## <a name="HandleFailures"></a>失败处理
由于MMA会自动归档日志，以下```grep```命令请根据实际情况替换为```zgrep```命令。


执行以下命令查看当前迁移失败的表：
```$xslt
$ /path/to/bin/mma-client --list FAILED
```

假设失败的表为```test_db.test_table```，我们可以执行以下命令从所有MMA server的日志中获取失败的原因：
```$xslt
$ grep "FAILED" /path/to/mma/log/mma_server.LOG* | grep "test_db.test_table"
```
输出可能包含多条日志，我们需要的是最后一条Update action progress的日志，如：
```$xslt
2020-10-26 18:03:39,380 INFO  [Scheduler] action.AbstractAction (AbstractAction.java:setProgress(121)) - Update action progress, id: Migration.test_db.test_table.1603706572.DataTransfer, cur progress: RUNNING, new progress: FAILED
```

通过以上日志，我们可以看到是```Migration.test_db.test_table.1603706572.DataTransfer```这个DataTransfer
action执行失败了。

接下来，我们将介绍各种Action可能的失败原因，以及调查方法。

### CreateTable
这个Action失败通常因为MaxCompute中没有打开新类型开关，请参考[准备工作](#Preparation)中的MaxCompute配置一节。

调查方法：
根据Action ID在mma/log/task_runner.LOG中查找DDL在MC中执行的logview。命令为
```$xslt
$ grep "${ACTION_ID}" /path/to/mma/log/action_executor.LOG
```
输出样例：
```$xslt
2020-10-26 18:03:47,658 [ActionExecutor- #17] ActionId: Migration.mma_test.dummy.1603706621.CreateTable, InstanceId: 20201026100347413gvsu46pr2
2020-10-26 18:03:47,695 [ActionExecutor- #17] ActionId: Migration.mma_test.dummy.1603706621.CreateTable, LogView http://logview.odps.aliyun.com/logview/?h=http://service.cn.maxcompute.aliyun-inc.com/api&p=odps_mma_test&i=20201026100347413gvsu46pr2&token=SC83c2JOODVtWG9XT3BKSWxPZTNoNVdYM0R3PSxPRFBTX09CTzoxNTU4MzkxOTQ2NTYxODIxLDE2MDM5NjU4MjcseyJTdGF0ZW1lbnQiOlt7IkFjdGlvbiI6WyJvZHBzOlJlYWQiXSwiRWZmZWN0IjoiQWxsb3ciLCJSZXNvdXJjZSI6WyJhY3M6b2RwczoqOnByb2plY3RzL29kcHNfbW1hX3Rlc3QvaW5zdGFuY2VzLzIwMjAxMDI2MTAwMzQ3NDEzZ3ZzdTQ2cHIyIl19XSwiVmVyc2lvbiI6IjEifQ==
```
此时可以将在浏览器中打开logview URL，即可看到具体失败原因。

### AddPartition
这个Action可能因为元数据并发操作太多导致失败，在绝大多数情况下可以靠MMA自动重试解决，用户无需介入。

调查方法同CreateTable。

### DataTransfer
这个Action可能的失败情况比较多，常见的如下：
1. Hive UDTF没有正确创建，请参考请参考[配置](#Configuration)中创建UDTF部分
1. 集群网络环境问题，MapReduce任务报错如UnknownHost（DNS问题），或Connection Timeout（Endpoint配置或路由问题）
1. string超过8MB，这个问题请提交工单解决
1. 脏数据，此时这张表或分区往往已经无法正常读出数据
1. 并发数量高，压力大导致失败，由于MMA自动重试机制，这个问题目前很少出现

调查方法：首先，根据Action ID在mma/log/mma_server.LOG中查找报错，命令为：
```$xslt
$ grep "${ACTION_ID}" /path/to/mma/log/mma_server.LOG* | grep "stack trace"
```
输出中会包含失败原因，如：
```$xslt
2020-10-27 12:16:08,615 ERROR [FinishedActionHandler] action.HiveSqlAction (HiveSqlAction.java:afterExecution(60)) - Action failed, actionId: Migration.mma_test.dummy.1603772154.DataTransfer, stack trace: java.util.concurrent.ExecutionException: org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: SemanticException [Error 10011]: Invalid function odps_data_dump_multi
```

如果失败原因为MapReduce Job执行失败，则需要查找MapReduce Job失败原因。根据Action ID在mma/log/action_executor.LOG中
查找Hive SQL的tracking URL。命令为
```$xslt
$ grep "${ACTION_ID}" /path/to/mma/log/action_executor.LOG
```
输出样例：
```$xslt
2020-10-26 16:38:20,116 [Thread-12] ActionId: Migration.mma_test.test_partitioned_100x10k.1603701412.5.DataTransfer, jobId:  job_1591948285564_0267
2020-10-26 16:38:20,116 [Thread-12] ActionId: Migration.mma_test.test_partitioned_100x10k.1603701412.5.DataTransfer, tracking url:  http://emr-header-1.cluster-177129:20888/proxy/application_1591948285564_0267/
```
根据上面的信息，可以在yarn上查找这个MapReduce Job的日志。

### SourceVerification
这个Action失败通常与Hive集群相关。

调查方法：根据Action ID在mma/log/action_executor.LOG中查找Hive SQL的tracking URL。命令为
```$xslt
$ grep "${ACTION_ID}" /path/to/mma/log/action_executor.LOG
```
输出样例：
```$xslt
2020-10-26 16:38:20,116 [Thread-12] ActionId: Migration.mma_test.test_partitioned_100x10k.1603701412.5.SourceVerification, jobId:  job_1591948285564_0267
2020-10-26 16:38:20,116 [Thread-12] ActionId: Migration.mma_test.test_partitioned_100x10k.1603701412.5.SourceVerification, tracking url:  http://emr-header-1.cluster-177129:20888/proxy/application_1591948285564_0267/
```

### DestVerification
这个Action失败通常与MC相关。

调查方法：
根据Action ID在mma/log/action_executor.LOG中查找DDL在MC中执行的logview。命令为
```$xslt
$ grep "${ACTION_ID}" /path/to/mma/log/action_executor.LOG
```
输出样例：
```$xslt
2020-10-26 18:03:47,658 [ActionExecutor- #17] ActionId: Migration.mma_test.dummy.1603706621.DestVerification, InstanceId: 20201026100347413gvsu46pr2
2020-10-26 18:03:47,695 [ActionExecutor- #17] ActionId: Migration.mma_test.dummy.1603706621.DestVerification, LogView http://logview.odps.aliyun.com/logview/?h=http://service.cn.maxcompute.aliyun-inc.com/api&p=odps_mma_test&i=20201026100347413gvsu46pr2&token=SC83c2JOODVtWG9XT3BKSWxPZTNoNVdYM0R3PSxPRFBTX09CTzoxNTU4MzkxOTQ2NTYxODIxLDE2MDM5NjU4MjcseyJTdGF0ZW1lbnQiOlt7IkFjdGlvbiI6WyJvZHBzOlJlYWQiXSwiRWZmZWN0IjoiQWxsb3ciLCJSZXNvdXJjZSI6WyJhY3M6b2RwczoqOnByb2plY3RzL29kcHNfbW1hX3Rlc3QvaW5zdGFuY2VzLzIwMjAxMDI2MTAwMzQ3NDEzZ3ZzdTQ2cHIyIl19XSwiVmVyc2lvbiI6IjEifQ==
```
此时可以将在浏览器中打开logview URL，即可看到具体失败原因。

### Compare
这个Action失败通常因为MC和Hive中数据不一致，MMA的重试机制通常可以自动解决这个问题。

调查方法：
根据Action ID在mma/log/mma_server.LOG中查找日志。命令为
```$xslt
$ grep "${ACTION_ID}" /path/to/mma/log/mma_server.LOG
```
输出样例：
```$xslt
2020-10-27 14:56:37,781 ERROR [Scheduler] action.AbstractAction (VerificationAction.java:execute(66)) - Record number not matched, source: 1, dest: 2, actionId: Migration.mma_test.dummy.1603781749.Compare
```

## FAQ
## 1. 升级MMA
MMA会不断更新功能，并修复已知问题，提高稳定性，因此我们建议长期使用MMA的客户升级MMA。升级MMA的步骤如下：
1. 下载解压新版本MMA
1. 停止老版本MMA
1. 将已有的MMA根目录下的元数据文件.MmaMeta.mv.db复制到新版本MMA的根目录下
1. 启动新版本MMA

## 2. 迁移速率慢
可能的原因包括：
- 公共云限流：最大允许的带宽为2Gbps，如果需要调整限流阈值请提工单
- Endpoint配置：注意区分公网和阿里云内网Endpoint

## 3. 导入到多个MC project
需要保证在执行mma/bin/configure的时候，填入的阿里云账号有这些project的admin权限。之后在配置table mapping时就可以
选择将数据导入不同project了。请参考[生成任务配置](#GenerateJobConfig)

## 4. 进度条一直不动
目前进度条是基于完成的分区数量显示进度的，因此会出现跳变的情况（对于非分区表会直接从0跳到100）。这一点将在后续版本优化，
目前可以通过```tailf mma/log/mma_server.LOG```来监控。
