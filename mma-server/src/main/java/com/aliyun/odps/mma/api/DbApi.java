package com.aliyun.odps.mma.api;

import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.constant.JobType;
import com.aliyun.odps.mma.execption.JobSubmittingException;
import com.aliyun.odps.mma.model.*;
import com.aliyun.odps.mma.orm.JobProxy;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.query.DbFilter;
import com.aliyun.odps.mma.query.JobFilter;
import com.aliyun.odps.mma.service.DataSourceService;
import com.aliyun.odps.mma.service.DbService;
import com.aliyun.odps.mma.service.PartitionService;
import com.aliyun.odps.mma.service.TableService;
import com.aliyun.odps.mma.validator.ValidateJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Validated
@RestController
@RequestMapping("/api/dbs")
public class DbApi {
    private final DataSourceService dsService;
    private final DbService dbService;
    private final TableService tbService;
    private final PartitionService ptService;
    private final OrmFactory ormFactory;

    @Autowired
    public DbApi(
            DataSourceService dsService,
            DbService dbService,
            TableService tbService,
            PartitionService ptService,
            OrmFactory ormFactory
    ) {
        this.dsService = dsService;
        this.dbService = dbService;
        this.tbService = tbService;
        this.ptService = ptService;
        this.ormFactory = ormFactory;
    }

    @PutMapping("")
    public ApiRes getDbs(@RequestBody DbFilter dbFilter) {
        int dbsCount = dbService.getDbsCount(dbFilter);

        List<DataBaseModel> dbs = dbService.getDbs(dbFilter);
        if (dbs.size() == 0) {
            return ApiRes.ok("data", dbs);
        }

        List<Integer> dbIds = dbs.stream().map(DataBaseModel::getId).collect(Collectors.toList());

        Map<Integer, DataBaseModel> dbMap = new HashMap<>();
        for (DataBaseModel db: dbs) {
            dbMap.put(db.getId(), db);
        }

        List<Map<String, Object>> ptStatList = ptService.ptStatOfDbs(dbIds);

        for (Map<String, Object> ptStat: ptStatList) {
            Integer dbId = (Integer) ptStat.get("dbId");
            DataBaseModel db = dbMap.get(dbId);
            int count = ((Long)ptStat.get("count")).intValue();
            db.setPartitions(db.getPartitions() + count);

            switch ((String) ptStat.get("status")) {
                case "INIT":
                    break;
                case "DOING":
                    db.setPartitionsDoing(count);
                    break;
                case "DONE":
                    db.setPartitionsDone(count);
                    break;
                case "FAILED":
                    db.setPartitionsFailed(count);
                    break;
            }
        }

        List<Map<String, Object>> tableStatList = tbService.tableStatOfDbs(dbIds);

        for (Map<String, Object> tableStat: tableStatList) {
            Integer dbId = (Integer) tableStat.get("dbId");
            DataBaseModel db = dbMap.get(dbId);
            int count = ((Long)tableStat.get("count")).intValue();
            db.setTables(db.getTables() + count);

            switch ((String) tableStat.get("status")) {
                case "INIT":
                    break;
                case "DOING":
                    db.setTablesDoing(count);
                    break;
                case "DONE":
                    db.setTablesDone(count);
                    break;
                case "FAILED":
                    db.setTablesFailed(count);
                    break;
                case "PART_DONE":
                    db.setTablesPartDone(count);
                    break;
            }
        }

        ApiRes apiRes = ApiRes.ok();
        apiRes.addData("total", dbsCount);
        apiRes.addData("data", dbs);

        return apiRes;
    }

    @GetMapping("/{dbId}")
    public DataBaseModel getDbById(@PathVariable("dbId") int dbId) {
        Optional<DataBaseModel> dmOpt = dbService.getDbById(dbId);

        if (! dmOpt.isPresent()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        return dmOpt.get();
    }

    @PutMapping("/{dbId}/job")
    public ApiRes submitDbJob(
            @PathVariable("dbId") int dbId,
            @ValidateJob @RequestBody JobModel jobModel
    ) throws JobSubmittingException {
        DataBaseModel db = this.getDbById(dbId);
        JobProxy job = ormFactory.newJobProxy(jobModel);

        // 补全job信息，这里的信息只用来做记录
        jobModel.setDbName(db.getName());

        DataSourceModel ds = dsService.getDataSource(db.getSourceId()).get();
        jobModel.setSourceName(ds.getName());
        jobModel.setType(JobType.Database);

        int jobId = job.submitDb(db);

        return ApiRes.ok("job_id", jobId);
    }


}
