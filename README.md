# MaxCompute Migration Assistant
![build](https://github.com/aliyun/alibabacloud-maxcompute-tool-migrate/actions/workflows/build.yml/badge.svg)

MaxCompute Migration Assistant (MMA) provides a solution to migrate data from different datasources 
to MaxCompute.

Currently, the following scenario are supported:
- Hive to MaxCompute
- MaxCompute to OSS
- OSS to MaxCompute


## Build
To build MMA, the following dependencies are required:
- JDK 8 or newer

Execute the following command to build MMA:
```$xslt
$ mvn -U clean package -DskipTests
```

```distribution/target/mma-${project.version}.zip``` should be generated.

## Documents
[中文文档](https://github.com/aliyun/alibabacloud-maxcompute-tool-migrate/blob/master/documents/HiveToMaxCompute_zh_v0.1.0.md)

## License
licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
