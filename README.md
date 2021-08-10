# MaxCompute Migration Assistant
![build](https://github.com/aliyun/alibabacloud-maxcompute-tool-migrate/actions/workflows/build.yml/badge.svg)

MaxCompute Migration Assistant (MMA) provides a solution to migrate data from different datasources 
to MaxCompute. Currently, migration from Hive to MaxCompute is the most common scenario.

## Build
To build MMA, the following dependencies are required:
- JDK 8 or newer
- Python 3

Execute the following command to build MMA:
```$xslt
$ sh build.sh
```

A file named ```mma.tar.gz``` should be generated.

## Documents
[中文文档](https://github.com/aliyun/alibabacloud-maxcompute-tool-migrate/blob/master/documents/HiveToMaxCompute_zh.md)

## Test
Currently, our function test covers the Hive to MaxCompute scenario. The following dependencies are 
required:
- Hive client
- Python3

### Build test package
Build MMA with an extra option "--test":
```$xslt
$ sh build.sh --test
```

### Configure
Execute ```mma/bin/configure``` to run the setup wizard.

### Initialize dataset
Execute the following command to initialize a dataset:
```$xslt
$ python3 mma/test/setup.py
```
It may take one hour to build the test dataset.

### Run test cases
Execute the following command to run all the test cases:
```$xslt
$ python3 mma/test/test.py
```

### Advanced usages
The following features are also supported.
- Run a single test suite
- Run a single test case
- Fail fast

Execute the following command to see all the supported options:
```$xslt
$ python3 mma/test/test.py --help
```

## License
licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
