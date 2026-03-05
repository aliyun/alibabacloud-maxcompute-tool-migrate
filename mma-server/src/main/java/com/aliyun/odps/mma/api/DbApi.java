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
@RequestMapping("api/sources/{sourceId}/dbs")
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
    public ApiRes getDbs(
            @PathVariable("sourceId") int sourceId,
            @RequestBody DbFilter dbFilter
    ) {
        dbFilter.setSourceId(sourceId);
        int dbsCount = dbService.getDbsCount(dbFilter);

        List<DataBaseModel> dbs = dbService.getDbs(dbFilter);
        if (dbs.isEmpty()) {
            return ApiRes.ok("data", dbs);
        }

        ApiRes apiRes = ApiRes.ok();
        apiRes.addData("total", dbsCount);
        apiRes.addData("data", dbs);

        return apiRes;
    }

    @GetMapping("/{dbId}")
    public  DataBaseModel getDbById(
            @PathVariable("sourceId") int sourceId,
            @PathVariable("dbId") int dbId
    ) {
        Optional<DataBaseModel> dmOpt = dbService.getDbById(sourceId, dbId);

        if (! dmOpt.isPresent()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        return dmOpt.get();
    }

    @PutMapping("/{dbId}/job")
    public ApiRes submitDbJob(
            @PathVariable("sourceId") int sourceId,
            @PathVariable("dbId") int dbId,
            @ValidateJob @RequestBody JobModel jobModel
    ) throws JobSubmittingException {
        DataBaseModel db = this.getDbById(sourceId, dbId);
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
