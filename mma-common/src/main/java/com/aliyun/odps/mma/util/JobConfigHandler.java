package com.aliyun.odps.mma.util;

import com.aliyun.odps.mma.config.JobConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class JobConfigHandler extends BaseTypeHandler<JobConfig> {
    private static final Logger logger = LoggerFactory.getLogger(JobConfigHandler.class);
    private ObjectMapper om;

    public JobConfigHandler() {
        om = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void setNonNullParameter(PreparedStatement preparedStatement, int i, JobConfig config, JdbcType jdbcType) throws SQLException {
        try {
            preparedStatement.setString(i, om.writeValueAsString(config));
        } catch (JsonProcessingException e) {
            logger.error("", e);
            throw  new SQLException(e.getMessage());
        }
    }

    @Override
    public JobConfig getNullableResult(ResultSet resultSet, String s) throws SQLException {
        try {
            String json = resultSet.getString(s);
            return om.readValue(json, JobConfig.class);
        } catch (JsonProcessingException e) {
            logger.error("", e);
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public JobConfig getNullableResult(ResultSet resultSet, int i) throws SQLException {
        try {
            String json = resultSet.getString(i);
            return om.readValue(json, JobConfig.class);
        } catch (JsonProcessingException e) {
            logger.error("", e);
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public JobConfig getNullableResult(CallableStatement callableStatement, int i) throws SQLException {
        try {
            String json = callableStatement.getString(i);
            return om.readValue(json, JobConfig.class);
        } catch (JsonProcessingException e) {
            logger.error("", e);
            throw new SQLException(e.getMessage());
        }
    }
}
