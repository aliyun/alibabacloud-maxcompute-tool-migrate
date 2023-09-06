package com.aliyun.odps.mma.api;

import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.constant.TaskTypeName;
import com.aliyun.odps.mma.execption.JobSubmittingException;
import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.orm.JobProxy;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.query.JobFilter;
import com.aliyun.odps.mma.service.*;
import com.aliyun.odps.mma.service.JobService;
import com.aliyun.odps.mma.service.Service;
import com.aliyun.odps.mma.service.TaskService;
import com.aliyun.odps.mma.task.TaskManager;
import com.aliyun.odps.mma.validator.ValidateJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

@Validated
@RestController
@RequestMapping("/api/jobs")
public class JobApi {
    private final Service service;
    private final OrmFactory proxyFactory;
    private final TaskManager taskManager;
    private MMAConfig mmaConfig;

    @Autowired
    public JobApi(Service service, OrmFactory proxyFactory, TaskManager taskManager, MMAConfig mmaConfig) {
        this.service = service;
        this.proxyFactory = proxyFactory;
        this.taskManager = taskManager;
        this.mmaConfig = mmaConfig;
    }

    @GetMapping("")
    public ApiRes getJobs(JobFilter jobFilter) {
        JobService js = service.getJobService();
        TaskService ts = service.getTaskService();

        jobFilter.setJobIds(ts.getJobIdsByJobFilter(jobFilter));

        int jobsCount = js.getJobsCount(jobFilter);
        List<JobModel> jobs = js.getJobs(jobFilter);

        ApiRes apiRes = ApiRes.ok();
        apiRes.addData("data", jobs);
        apiRes.addData("total", jobsCount);

        return apiRes;
    }

    @GetMapping("/{jobId}")
    public JobModel getJobById(@PathVariable("jobId") int jobId) {
        JobModel job = this.service.getJobService().getJobById(jobId);

        if (Objects.isNull(job)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        return job;
    }

    @PostMapping("")
    public ApiRes submitJob(
            @ValidateJob(require = {"sourceName", "dbName", "type"})
            @RequestBody JobModel jobModel
    ) throws JobSubmittingException {
        JobProxy job = proxyFactory.newJobProxy(jobModel);
        int jobId = job.submit();

        return ApiRes.ok("job_id", jobId);
    }

    @PutMapping("/{jobId}")
    public ApiRes updateJob(@PathVariable("jobId") int jobId, @RequestParam("action") String action) {
        action = action.toLowerCase();

        switch (action) {
            case "stop":
                taskManager.cancelJob(jobId);
                break;
            case "retry":
                service.getJobService().retryJob(jobId);
                break;
            case "delete":
                taskManager.deleteJob(jobId);
                break;
            default:
                return ApiRes.error("unknown action " + action, null);
        }

        return ApiRes.ok();
    }


    @GetMapping("/options")
    public ApiRes getJobOptions(@RequestParam("ds_name") String dsName, @RequestParam("db_name") String dbName) {
        DbService ds = service.getDbService();
        Optional<DataBaseModel> dmOpt = ds.getDbByName(dsName, dbName);
        if (!dmOpt.isPresent()) {
            return ApiRes.ok();
        }

        DataBaseModel dm = dmOpt.get();
        int sourceId = dm.getSourceId();
        DataSourceService dsService = service.getDsService();;
        Optional<DataSourceModel> dsmOpt = dsService.getDataSource(sourceId);

        if (!dsmOpt.isPresent()) {
            return ApiRes.ok();
        }

        DataSourceModel dsm = dsmOpt.get();
        List<String> dstMcProjects = mmaConfig.getDstMcProjects();
        Map<TaskType, String> taskTypes = new HashMap<>();
        TaskType defaultTaskType = null;

        switch (dsm.getType()) {
            case HIVE:
                taskTypes.put(TaskType.HIVE, TaskTypeName.getName(TaskType.HIVE));
                taskTypes.put(TaskType.HIVE_MERGED_TRANS, TaskTypeName.getName(TaskType.HIVE_MERGED_TRANS));
                defaultTaskType = TaskType.HIVE;
                break;
            case HIVE_OSS:
                taskTypes.put(TaskType.HIVE_OSS, TaskTypeName.getName(TaskType.HIVE_OSS));
                taskTypes.put(TaskType.HIVE, TaskTypeName.getName(TaskType.HIVE));
                defaultTaskType = TaskType.HIVE_OSS;
                break;
            case OSS:
                break;
            case ODPS:
                taskTypes.put(TaskType.ODPS, TaskTypeName.getName(TaskType.ODPS));
                taskTypes.put(TaskType.ODPS_INSERT_OVERWRITE, TaskTypeName.getName(TaskType.ODPS_INSERT_OVERWRITE));
                taskTypes.put(TaskType.MC2MC_VERIFY, TaskTypeName.getName(TaskType.MC2MC_VERIFY));
                taskTypes.put(TaskType.ODPS_MERGED_TRANS, TaskTypeName.getName(TaskType.ODPS_MERGED_TRANS));
                defaultTaskType = TaskType.ODPS_INSERT_OVERWRITE;
                break;
            default:
                break;
        }

        TableService ts = service.getTableService();
        List<String> tableNames = ts.getTableNamesByDbId(dm.getId());

        Map<String, Object> options = new HashMap<>();
        options.put("dstMcProjects", dstMcProjects);
        options.put("taskTypes", taskTypes);
        options.put("defaultTaskType", defaultTaskType);
        options.put("tables", tableNames);

        return ApiRes.ok("data", options);
    }

    @GetMapping("/basic")
    public ApiRes getJobBasicInfo() {
        Map<Integer, String> jobIdToDec = service.getJobService().getIdToDecOfJobs();

        return ApiRes.ok("data", jobIdToDec);
    }
}
