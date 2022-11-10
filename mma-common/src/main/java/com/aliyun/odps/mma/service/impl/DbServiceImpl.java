package com.aliyun.odps.mma.service.impl;

import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.mapper.DbMapper;
import com.aliyun.odps.mma.query.DbFilter;
import com.aliyun.odps.mma.service.DbService;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
public class DbServiceImpl implements DbService {
    SqlSessionFactory sqlSessionFactory;
    private final DbMapper mapper;

    @Autowired
    public DbServiceImpl(SqlSessionFactory sqlSessionFactory, DbMapper mapper) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.mapper = mapper;
    }

    @Override
    public List<DataBaseModel> getAllDbs() {
        List<DataBaseModel> dbs = this.mapper.getAllDbs();

        if (Objects.nonNull(dbs)) {
            return dbs;
        }

        return Collections.emptyList();
    }

    @Override
    public List<DataBaseModel> getDbsOfDataSource(String sourceName) {
        return mapper.getDbsOfDataSource(sourceName);
    }

    @Override
    public Optional<DataBaseModel> getDbById(int id) {
        DataBaseModel db = this.mapper.getDbById(id);
        return Optional.ofNullable(db);
    }

    @Override
    public Optional<DataBaseModel> getDbByName(String dsName, String dbName) {
        DataBaseModel db = this.mapper.getDbByName(dsName, dbName);
        return Optional.ofNullable(db);
    }

    @Override
    public void insertDb(DataBaseModel db) {
        this.mapper.insertDb(db);
    }

    @Override
    public void updateDb(DataBaseModel db) {
        this.mapper.updateDb(db);
    }

    @Override
    public void batchUpdateDb(List<DataBaseModel> dbList) {
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            DbMapper mapper = sqlSession.getMapper(DbMapper.class);

            for (DataBaseModel db: dbList) {
                mapper.updateDb(db);
            }

            sqlSession.commit();
        }
    }

    @Override
    public List<DataBaseModel> getDbs(DbFilter dbFilter) {
        return this.mapper.getDbs(dbFilter);
    }

    @Override
    public int getDbsCount(DbFilter dbFilter) {
        return this.mapper.getDbsCount(dbFilter);
    }
}
