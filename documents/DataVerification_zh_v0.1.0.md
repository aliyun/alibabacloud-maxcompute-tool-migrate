使用"数据校验功能"补充说明

# 说明
数据校验会以分区或表为单位计算源、目标的哈希值，通过哈希值判定数据是否传输成功。目前仅支持从hive->maxcompute数据迁移的校验。
由于会花费额外的时间，可以通过配置"mma.data.enable.verification"配置来启停数据校验。 

# 配置
mma_server_config.json添加: "mma.data.enable.verification": "true",

# 创建 UDTF
数据校验是通过UDTF计算的, 所有需要提前在Hive和Maxcompute上创建UDTF

## 创建 Hive UDTF
配置过程中还需要将某些文件上传至 HDFS，并在 beeline 中创建 MMA 需要的 Hive 永久函数。MMA 配置引导脚本会自动生成需要执行的命令，直接复制粘贴到安装有 hdfs 命令与 beeline 的服务器上执行即可。命令示例如下：

上传 Hive UDTF Jar 包至 HDFS：

```shell
hdfs dfs -put -f ${MMA_HOME}/res/data-transfer-hive-udtf-${MMA_VERSION}-jar-with-dependencies.jar hdfs:///tmp/
```

使用 beeline 创建 Hive 函数：

```sql
DROP FUNCTION IF EXISTS default.odps_data_dump_multi;
CREATE FUNCTION default.odps_data_dump_multi as 'com.aliyun.odps.mma.io.McDataTransmissionUDTF' USING JAR 'hdfs:///tmp/data-transfer-hive-udtf-${MMA_VERSION}-jar-with-dependencies.jar';
```

## 创建 MaxCompute UDTF
在odpscmd端执行下面命令

## 1. 向MaxCompute添加jar类型资源 
```shell
add jar ${MMA_HOME}/res/odps-table-hasher-${MMA_VERSION}.jar -f
```

## 2. 创建UDTF函数
```shell
create function mma_hash_table as 'com.aliyun.odps.mma.validation.OdpsTableHasherUDTF' using 'odps-table-hasher-${MMA_VERSION}.jar';
```
