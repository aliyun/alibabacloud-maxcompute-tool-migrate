# SQL-checker 使用文档

## 使用方式

```console
$ mma/bin/sql-checker -h
usage: sql-checker [-d | -f | -q] [-s]
 -d,--directory <Path>      Directory that contains SQL scripts
 -f,--file <Path>           Script to check
 -h,--help                  Print usage
 -q,--query <Query>         Query to check
 -s,--settings <Settings>   Comma-separated MaxCompute SQL settings, like
                            odps.sql.type.system.odps2=true
```

- 其中，`-q`，`-f`，`-d` 三个选项用于指定待检查的 query。
  - `-q` 选项用于直接输入 query，
  - `-f` 选项用于指定 SQL 脚本所在路径，
  - `-d` 选项用于指定包含 SQL 脚本（特指以 .sql 结尾的文件）的目录。
  - 三个选项互斥，每次执行只能选择其中一个。
- `-s` 选项用于提供逗号分隔的MaxCompute SQL配置。

## 示例

### 检查单条query

sql-checker 直接输出此 query 的兼容性信息：

```console
$ mma/bin/sql-checker -q "select * test_db.test_tbl;"
(1/4) Loading cache directory: /root/mma/tmp/meta_cache
Succeeded
(2/4) Loading Hive configuration: /root/mma/conf/hive_config.ini
Succeeded
(3/4) Connecting to Hive MetaStore
Connected
(4/4) Checking query compatibility
Checking query (1/1)
Query index: 1
Query: select * from mma_test.dummy;
Overall Compatibility Level: OK
Issues:
```

### 检查SQL脚本中所有query

sql-checker 输出此脚本兼容性总结，此时每条 query 的兼容性信息可在 /root/result_1625470693101.txt 中查询，/root/error_1625470693101.txt 中包含了执行检查过程中的非预期错误，通常为空。样例输出如下：

```console
$ mma/bin/sql-checker -f /root/mingyou/scripts/script.sql
(1/4) Loading cache directory: /root/mma/tmp/meta_cache
Succeeded
(2/4) Loading Hive configuration: /root/mma/conf/hive_config.ini
Succeeded
(3/4) Connecting to Hive MetaStore
Connected
(4/4) Checking query compatibility
Checking script: /path/to/script.sql
Output path: /root/result_1625470693101.txt
Error output path: /root/error_1625470693101.txt
Checking query (1/2)
Checking query (2/2)
Script compatibility summary
Script path: /root/mingyou/scripts/script.sql
Number of queries: 2
Compatibility: OK(1) WEEK_WARNINGS(1)
Compatibility ratio: OK(50.00%) WEEK_WARNINGS(50.00%)
```

result_1625470693101.txt 内容如下：

```
Script path: /root/mingyou/scripts/script.sql
Query index: 1
Query: select * from mma_test.dummy;
Overall Compatibility Level: OK
Issues:

Script path: /root/mingyou/scripts/script.sql
Query index: 2
Query: SELECT orc.t_tinyint, count(1) from mma_test.test_orc_1x1k as orc join mma_test.test_parquet_1x1k as parquet on orc.t_tinyint=pa ...
Overall Compatibility Level: WEEK_WARNINGS
Issues:
(1/2) General issue (weak warning)
	Compatibility Level: WEEK_WARNINGS
	Description: [line 1, col 37] precision and scale is not currently supported in current mode, 'set odps.sql.decimal.odps2=true' to enable
(2/2) General issue (weak warning)
	Compatibility Level: WEEK_WARNINGS
	Description: [line 1, col 72] precision and scale is not currently supported in current mode, 'set odps.sql.decimal.odps2=true' to enable
Transformed Query: N/A
```

### 检查目录下所有SQL脚本

sql-checker 输出此目录下各脚本的兼容性总结，并在最后汇总为该目录的兼容性总结。每条 query 的兼容性信息依旧在 result 文件中查询。样例输出如下：

```console
$ mma/bin/sql-checker -d /root/mingyou/scripts/
(1/4) Loading cache directory: /root/mma/tmp/meta_cache
Succeeded
(2/4) Loading Hive configuration: /root/mma/conf/hive_config.ini
Succeeded
(3/4) Connecting to Hive MetaStore
Connected
(4/4) Checking query compatibility
Input dir: /root/mingyou/scripts
Checking script: /root/mingyou/scripts/script.1.sql
Output path: /root/result_1625470771467.txt
Error output path: /root/error_1625470771467.txt
Checking query (1/2)
Checking query (2/2)
Checking script: /root/mingyou/scripts/script.sql
Output path: /root/result_1625470771467.txt
Error output path: /root/error_1625470771467.txt
Checking query (1/2)
Checking query (2/2)
Script compatibility summary
Script path: /root/mingyou/scripts/script.1.sql
Number of queries: 2
Compatibility: OK(2)
Compatibility ratio: OK(100.00%)
Script compatibility summary
Script path: /root/mingyou/scripts/script.sql
Number of queries: 2
Compatibility: OK(1) WEEK_WARNINGS(1)
Compatibility ratio: OK(50.00%) WEEK_WARNINGS(50.00%)
Directory compatibility summary
Directory path: /root/mingyou/scripts
Number of scripts: 2
Number of queries: 4
Compatibility: OK(3) WEEK_WARNINGS(1)
Compatibility ratio: OK(75.00%) WEEK_WARNINGS(25.00%)
```

### 指定 MCQL settings

```console
$ mma/bin/sql-checker -d sample-queries-tpcds/ -s "odps.sql.decimal.odps2=true,odps.sql.type.system.odps2=true,odps.sql.validate.orderby.limit=false,odps.sql.timezone=UTC"
```

通常进行兼容性检查时，通过指定一些MCQL settings可以绕过不兼容的问题。常见的如：

```properties
# 以下三个配置指定使用MC 2.0类型系统
odps.sql.type.system.odps2=true
odps.sql.decimal.odps2=true
odps.sql.hive.compatible=false
# 提供一个默认Timezone
odps.sql.timezone=UTC
```