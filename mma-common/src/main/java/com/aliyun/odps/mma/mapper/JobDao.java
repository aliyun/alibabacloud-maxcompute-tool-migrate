package com.aliyun.odps.mma.mapper;

import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.jdbc.IntSetter;
import com.aliyun.odps.mma.jdbc.Setter;
import com.aliyun.odps.mma.jdbc.SqlUtils;
import com.aliyun.odps.mma.model.JobBatchModel;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.model.TaskModel;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class JobDao {
    private static final Logger logger = LoggerFactory.getLogger(JobDao.class);

    public static void saveTaskAndPartitions(Connection conn, List<TaskModel> _tasks) throws SQLException {
        String sql1 = "insert into task_partition (job_id, task_id, partition_id) values (?, ?, ?)";
        String sql2 = "insert into task (" +
                "id, job_id, batch_id, source_id, db_id, table_id, " +
                "db_name, schema_name, table_name, " +
                "odps_project, odps_schema, odps_table, " +
                "type, status, create_time)" +
                " values (?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, " +
                "?, ?, ?," +
                "?, ?, now())"; // 14个参数
        List<Setter> taskPartitionSetters = new ArrayList<>();
        List<Setter> taskSetters = new ArrayList<>();
        for (TaskModel task : _tasks) {
            List<Integer> partitions = task.getPartitions();
            if (Objects.nonNull(partitions)) {
                for (Integer pid : partitions) {
                    taskPartitionSetters.add(new IntSetter(task.getJobId(), task.getId(), pid));
                }
            }
            taskSetters.add(new Setter() {
                @Override
                public void bind(PreparedStatement ps) throws SQLException {
                    ps.setInt(1, task.getId());
                    ps.setInt(2, task.getJobId());
                    ps.setInt(3, task.getBatchId());
                    ps.setInt(4, task.getSourceId());
                    ps.setInt(5, task.getDbId());
                    ps.setInt(6, task.getTableId());

                    ps.setString(7, task.getDbName());
                    ps.setString(8, task.getSchemaName());
                    ps.setString(9, task.getTableName());

                    ps.setString(10, task.getOdpsProject());
                    ps.setString(11, task.getOdpsSchema());
                    ps.setString(12, task.getOdpsTable());

                    ps.setString(13, task.getType().toString());
                    ps.setString(14, TaskStatus.INIT.toString());
                }
            });
        }
        SqlUtils.executeBatch(conn, sql1, taskPartitionSetters);
        SqlUtils.executeBatch(conn, sql2, taskSetters);
    }

    public static void saveJob(Connection conn, boolean jobIsNew, JobModel jobModel, JobBatchModel jobBatch) throws SQLException {
        // 等tasks, task_partitions提交完毕后，将job，jobBatch插入db，这时候tasks和job、jobBatch才对外可见
        // TODO, 插入task前，set job_batch.is_ok=false, 插入后，set job_batch.is_ok=true
        // 插入task后， set job_batch.is_ok=true

        if (jobIsNew) {
            String insertJob = "insert into job (" +
                    " id, description, " +
                    " source_name, db_name, odps_project, odps_schema, " +
                    " type, status, timer, config, create_time)" +
                    " values (?, ?, " +
                    " ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, now())";

            SqlUtils.executeUpdate(conn, insertJob, new Setter() {
                @Override
                public void bind(PreparedStatement ps) throws SQLException {
                    ps.setInt(1, jobModel.getId());
                    ps.setString(2, jobModel.getDescription());

                    ps.setString(3, jobModel.getSourceName());
                    ps.setString(4, jobModel.getDbName());
                    ps.setString(5, jobModel.getDstOdpsProject());
                    ps.setString(6, jobModel.getDstOdpsSchema());

                    ps.setString(7, jobModel.getType().name());
                    ps.setString(8, jobModel.getStatus().name());

                    if (Objects.nonNull(jobModel.getTimer())) {
                        ps.setString(9, new Gson().toJson(jobModel.getTimer()));
                    } else {
                        ps.setString(9, null);
                    }

                    ObjectMapper om = new ObjectMapper()
                            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    try {
                        ps.setString(10, om.writeValueAsString(jobModel.getConfig()));
                    } catch (JsonProcessingException e) {
                        logger.error("", e);
                        throw new SQLException(e.getMessage());
                    }
                }
            });
        }
        if (Objects.nonNull(jobModel.getDeleted()) && jobModel.getDeleted()) {
            // 有定时任务的场景下，可能存在job已经删除，但是恰好定时任务要提交新的子任务的情况
            //sqlSession.commit();
        } else {
            String insertJobBatch = "insert into job_batch" +
                    " (job_id, batch_id, status, err_msg, create_time) " +
                    " values (?,?,?,?, now())";
            SqlUtils.executeUpdate(conn, insertJobBatch, new Setter() {
                //#{jobId}, #{batchId}, #{status}, #{errMsg},
                @Override
                public void bind(PreparedStatement ps) throws SQLException {
                    ps.setInt(1, jobBatch.getJobId());
                    ps.setInt(2, jobBatch.getBatchId());
                    ps.setString(3, jobBatch.getStatus().name());
                    ps.setString(4, jobBatch.getErrMsg());
                }
            });
        }
    }
}
