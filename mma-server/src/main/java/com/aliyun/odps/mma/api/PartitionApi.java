package com.aliyun.odps.mma.api;

import com.aliyun.odps.mma.constant.JobType;
import com.aliyun.odps.mma.execption.JobSubmittingException;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.orm.JobProxy;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.query.PtFilter;
import com.aliyun.odps.mma.service.DataSourceService;
import com.aliyun.odps.mma.service.PartitionService;
import com.aliyun.odps.mma.validator.ValidateJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("/api/partitions")
@Validated
public class PartitionApi {
    private final DataSourceService dsService;
    private final PartitionService ps;
    private final OrmFactory proxyFactory;

    @Autowired
    public PartitionApi(DataSourceService dsService, PartitionService ps, OrmFactory proxyFactory) {
        this.dsService = dsService;
        this.ps = ps;
        this.proxyFactory = proxyFactory;
    }

    @PutMapping("")
    public ApiRes getPts(@RequestBody PtFilter ptFilter) {
        int ptsCount = ps.getPtsCount(ptFilter);
        List<PartitionModel> pts = ps.getPts(ptFilter);

        ApiRes apiRes = ApiRes.ok();
        apiRes.addData("total", ptsCount);
        apiRes.addData("data", pts);

        return  apiRes;
    }

    @PutMapping("/{ptId}/job")
    public ResponseEntity<ApiRes> submitSinglePartitionJob(
            @PathVariable("ptId") int ptId,
            @ValidateJob @RequestBody JobModel jobModel
    ) throws JobSubmittingException {
        PartitionModel pm = ps.getPartitionById(ptId);

        if (Objects.isNull(pm)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        JobProxy job = proxyFactory.newJobProxy(jobModel);

        // 补全job信息，这里的信息只用来做记录
        DataSourceModel ds = dsService.getDataSource(pm.getSourceId()).get();
        jobModel.setSourceName(ds.getName());
        jobModel.setDbName(pm.getDbName());
        jobModel.setType(JobType.Partitions);

        Integer jobId = job.submitPartition(pm);
        return new ResponseEntity<>(ApiRes.ok("job_id", jobId), HttpStatus.OK);
    }

    @PutMapping("/status")
    public ApiRes resetPtStatus(@RequestBody Map<String, List<Integer>> req) {
        List<Integer> ptIds = (List<Integer>) req.get("ptIds");
        int affected = ps.resetPtStatus(ptIds);
        return ApiRes.ok("data", affected);
    }
}
