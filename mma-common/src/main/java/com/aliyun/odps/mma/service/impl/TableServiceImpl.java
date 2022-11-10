package com.aliyun.odps.mma.service.impl;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.mapper.TableMapper;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.query.TableFilter;
import com.aliyun.odps.mma.service.TableService;
import com.aliyun.odps.mma.util.TableHasher;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
public class TableServiceImpl implements TableService {
    SqlSessionFactory sqlSessionFactory;
    TableMapper mapper;

    @Autowired
    public TableServiceImpl(SqlSessionFactory sqlSessionFactory, TableMapper mapper) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.mapper = mapper;
    }

    @Override
    public List<TableModel> getTablesOfDb(int dbId) {
        return this.mapper.getTablesOfDb(dbId);
    }

    @Override
    public List<TableModel> getTablesOfDataSource(String sourceName) {
        return this.mapper.getTablesOfDataSource(sourceName);
    }

    @Override
    public List<TableModel> getTablesOfDbWithWhiteList(int dbId, List<String> whiteList) {
        return this.mapper.getTablesOfDbWithWhiteList(dbId, whiteList);
    }

    @Override
    public List<TableModel> getTablesOfDbWithBlackList(int dbId, List<String> blackList) {
        return this.mapper.getTablesOfDbWithBlackList(dbId, blackList);
    }

    @Override
    public TableModel getTableById(int id) {
        return this.mapper.getTableById(id);
    }

    @Override
    public void insertTable(TableModel tm) {
        this.mapper.insertTable(tm);
    }

    @Override
    public void updateTable(TableModel tm) {
        this.mapper.updateTable(tm);
    }

    @Override
    @Transactional
    public void batchInsertTables(List<TableModel> tmList) {
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            TableMapper mapper = sqlSession.getMapper(TableMapper.class);

            for (TableModel tm: tmList) {
                mapper.insertTable(tm);
            }

            sqlSession.commit();
        }
    }

    @Override
    public void batchUpdateTables(List<TableModel> tmList) {
        if (Objects.isNull(tmList) || tmList.size() == 0) {
            return;
        }

        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            TableMapper mapper = sqlSession.getMapper(TableMapper.class);

            for (TableModel tm: tmList) {
                mapper.updateTable(tm);
            }

            sqlSession.commit();
        }
    }

    @Override
    public List<TableHasher> getAllTableHasher() {
        return mapper.getAllTableHasher();
    }

    @Override
    public List<TableModel> getTablesOfDbByNames(String dsName, String dbName, List<String> tableNames) {
        return mapper.getTablesOfDbByNames(dsName, dbName, tableNames);
    }

    @Override
    public List<TableModel> getAllTables() {
        List<TableModel> tables = mapper.getAllTables();

        if (Objects.nonNull(tables)) {
            return tables;
        }

        return Collections.emptyList();
    }

    @Override
    public List<TableModel> getTables(TableFilter tableFilter) {
        return mapper.getTables(tableFilter);
    }

    @Override
    public int getTablesCount(TableFilter tableFilter) {
        return mapper.getTablesCount(tableFilter);
    }

    @Override
    public List<TableModel> getTablesOfDbs(List<Integer> dbIds) {
        if (dbIds.size() == 0) {
            return new ArrayList<>();
        }
        return  mapper.getTablesOfDbs(dbIds);
    }

    @Override
    public void updateTableStatus(int tableId, MigrationStatus status) {
        mapper.updateTableStatus(tableId, status);
    }

    @Override
    public List<Map<String, Object>> tableStatOfDbs(List<Integer> dbIds) {
        return mapper.tableStatOfDbs(dbIds);
    }

    @Override
    public List<String> getTableNamesByDbId(int dbId) {
        List<String> names = this.mapper.getTableNamesByDbId(dbId);
        if (Objects.isNull(names)) {
            return new ArrayList<>();
        }

        return names;
    }

}
