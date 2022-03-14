# 说明
数据校验会以分区或表为单位计算源、目标的哈希值，通过哈希值判定数据是否传输成功。目前仅支持从hive->maxcompute数据迁移的校验。
由于会花费额外的时间，可以通过配置"mma.data.enable.verification"配置来启停数据校验。 

# 配置
mma_server_config.json添加: "mma.data.enable.verification": "true",
