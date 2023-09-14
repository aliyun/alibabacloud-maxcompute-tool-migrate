package com.aliyun.odps.mma.scheduling;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.mapper.DbMapper;
import com.aliyun.odps.mma.mapper.PartitionMapper;
import com.aliyun.odps.mma.mapper.TableMapper;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Component
public class MetaDataStatusUpdater {
    private final Logger logger = LoggerFactory.getLogger(MetaDataStatusUpdater.class);

    PartitionMapper ptMapper;
    TableMapper tableMapper;
    SqlSessionFactory sqlSessionFactory;
    Map<MigrationStatus, List<Integer>> statusToTables = new HashMap<>();
    Map<MigrationStatus, Boolean> tableStatusChanged = new HashMap<>();
    Map<MigrationStatus, List<Integer>> statusToDbs = new HashMap<>();
    Map<MigrationStatus, Boolean> dbStatusChanged = new HashMap<>();

    @Autowired
    public MetaDataStatusUpdater(PartitionMapper ptMapper, TableMapper tableMapper, SqlSessionFactory sqlSessionFactory) {
        this.ptMapper = ptMapper;
        this.tableMapper = tableMapper;
        this.sqlSessionFactory = sqlSessionFactory;
    }

    @Scheduled(fixedRate = 3000, initialDelay = 1000)
    public void updateMetaDataStatus() {
        // 通过partition状态更新分区表的状态，非分区表的状态直接由task executor来设置
        // select table_id as objId, status from partition_model group by table_id, status
        List<Map<String, Object>> ptStatList = ptMapper.ptStat();
        Map<MigrationStatus, List<Integer>> newStatusToTables = getStatusToMetaObj(ptStatList);
        boolean changed = hasStatusMapChanged(newStatusToTables, statusToTables, tableStatusChanged);

        if (changed) {
            statusToTables = newStatusToTables;
            updateTablesStatus(statusToTables);
        }

        // 根据table的状态更新db的状态
        // select db_id as objId, status from table_model group by db_id, status
        List<Map<String, Object>> tableStatList = tableMapper.tableStat();
        Map<MigrationStatus, List<Integer>> newStatusToDbs = getStatusToMetaObj(tableStatList);
        changed = hasStatusMapChanged(newStatusToDbs, statusToDbs, dbStatusChanged);

        if (changed) {
            statusToDbs = newStatusToDbs;
            updateDbsStatus(statusToDbs);
        }
    }

    Map<MigrationStatus, List<Integer>> getStatusToMetaObj(List<Map<String, Object>> statList) {
        Set<Integer> doneSet = new HashSet<>();
        Set<Integer> doingSet = new HashSet<>();
        Set<Integer> failedSet = new HashSet<>();
        Set<Integer> initSet = new HashSet<>();
        Set<Integer> partDoneSet = new HashSet<>();

        Map<MigrationStatus, Set<Integer>> statusMap = new HashMap<>();
        statusMap.put(MigrationStatus.DONE, doneSet);
        statusMap.put(MigrationStatus.DOING, doingSet);
        statusMap.put(MigrationStatus.FAILED, failedSet);
        statusMap.put(MigrationStatus.INIT, initSet);
        statusMap.put(MigrationStatus.PART_DONE, partDoneSet);

        Set<Integer> metaObjIds = new HashSet<>();

        for (Map<String, Object> stat: statList) {
            MigrationStatus status = MigrationStatus.valueOf((String) stat.get("status"));
            Integer objId = (Integer) stat.get("objId");
            metaObjIds.add(objId);

            statusMap.get(status).add(objId);
        }

        // table 状态计算, db的状态类似，只不是看table 的状态:
        // 1. partition有doing，状态为doing, 结束
        // 2. partition全都是done, 状态为done，结束
        // 3. partition全部为init, 状态为init, 结束
        // 4. partition有failed, 状态为failed, 结束
        // 5. partition有done, 有init, 状态为part_done, 结束

        // db 状态计算
        // 1. table有doing有doing，状态为doing, 结束
        // 2. table全都是done, 状态为done，结束
        // 3. table全部为init, 状态为init, 结束
        // 4. table有failed, 状态为failed, 结束
        // 5. table有done, 有init, 状态为part_done, 结束
        // 6. table有part_done, 状态为part_done结束

        Map<MigrationStatus, List<Integer>> statusToMataObj = new HashMap<>();
        statusToMataObj.put(MigrationStatus.INIT, new ArrayList<>());
        statusToMataObj.put(MigrationStatus.DOING, new ArrayList<>());
        statusToMataObj.put(MigrationStatus.DONE, new ArrayList<>());
        statusToMataObj.put(MigrationStatus.FAILED, new ArrayList<>());
        statusToMataObj.put(MigrationStatus.PART_DONE, new ArrayList<>());


        for (Integer objId: metaObjIds) {
            if (doingSet.contains(objId)) {
                statusToMataObj.get(MigrationStatus.DOING).add(objId);
                continue;
            }

            if (failedSet.contains(objId)) {
                statusToMataObj.get(MigrationStatus.FAILED).add(objId);
                continue;
            }

            if (partDoneSet.contains(objId)) {
                statusToMataObj.get(MigrationStatus.PART_DONE).add(objId);
                continue;
            }

            if (doneSet.contains(objId) && !initSet.contains(objId)) {
                statusToMataObj.get(MigrationStatus.DONE).add(objId);
                continue;
            }

            if (doneSet.contains(objId)) {
                statusToMataObj.get(MigrationStatus.PART_DONE).add(objId);
                continue;
            }

            statusToMataObj.get(MigrationStatus.INIT).add(objId);
        }

        return statusToMataObj;
    }

    @Transactional
    public void updateTablesStatus(Map<MigrationStatus, List<Integer>> statusToTable) {
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
            TableMapper tableMapper = sqlSession.getMapper(TableMapper.class);

            for (MigrationStatus status: statusToTable.keySet()) {
                if (! tableStatusChanged.containsKey(status)) {
                    continue;
                }
                List<Integer> tableIds = statusToTable.get(status);

                if (tableIds.size() > 0) {
                    tableMapper.batchUpdateTableStatus(status, tableIds);
                }
            }

            sqlSession.commit();
        }
    }

    @Transactional
    public void updateDbsStatus(Map<MigrationStatus, List<Integer>> statusTodb) {
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
            DbMapper dbMapper = sqlSession.getMapper(DbMapper.class);

            for (MigrationStatus status: statusTodb.keySet()) {
                if (! dbStatusChanged.containsKey(status)) {
                    continue;
                }

                List<Integer> dbIds = statusTodb.get(status);
                if (dbIds.size() > 0) {
                    dbMapper.batchUpdateDbsStatus(status, dbIds);
                }
            }

            sqlSession.commit();
        }
    }

    private boolean hasStatusMapChanged(
            Map<MigrationStatus, List<Integer>> newStatusMap,
            Map<MigrationStatus, List<Integer>> oldStatusMap,
            Map<MigrationStatus, Boolean> statusChanged
    ) {

        statusChanged.clear();

        for (MigrationStatus status: newStatusMap.keySet()) {
            List<Integer> newIds = newStatusMap.get(status);
            List<Integer> oldIds = oldStatusMap.getOrDefault(status, new ArrayList<>());

            if (newIds.size() != oldIds.size()) {
                statusChanged.put(status, true);
                continue;
            }

            newIds.sort(Comparator.comparingInt(a -> a));
            oldIds.sort(Comparator.comparingInt(a -> a));

            for (int i = 0, n = newIds.size(); i < n; i ++) {
                if (! newIds.get(i).equals(oldIds.get(i))) {
                    statusChanged.put(status, true);
                }
            }
        }

        return statusChanged.size() > 0;
    }
}
