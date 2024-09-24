package com.aliyun.odps.mma.api;

import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.constant.TaskTypeName;
import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.model.JobBatchModel;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.orm.JobProxy;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.query.JobFilter;
import com.aliyun.odps.mma.service.*;
import com.aliyun.odps.mma.service.JobService;
import com.aliyun.odps.mma.service.Service;
import com.aliyun.odps.mma.service.TaskService;
import com.aliyun.odps.mma.task.TaskManager;
import com.aliyun.odps.mma.util.I18nUtils;
import com.aliyun.odps.mma.util.OdpsUtils;
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
    private final MMAConfig mmaConfig;
    private final TaskTypeName taskTypeName;

    @Autowired
    public JobApi(Service service, OrmFactory proxyFactory, TaskManager taskManager, MMAConfig mmaConfig, TaskTypeName taskTypeName) {
        this.service = service;
        this.proxyFactory = proxyFactory;
        this.taskManager = taskManager;
        this.mmaConfig = mmaConfig;
        this.taskTypeName = taskTypeName;
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
    ) throws Exception {
        JobProxy job = proxyFactory.newJobProxy(jobModel);
        int jobId = job.submit();

        return ApiRes.ok("job_id", jobId);
    }

    @PutMapping("/{jobId}")
    public ApiRes updateJob(@PathVariable("jobId") int jobId, @RequestParam("action") String action) {
        action = action.toLowerCase();

        switch (action) {
            case "stop":
                // set job.stopped = 1
                // set task.running = 0 where task.running = 1
                taskManager.cancelJob(jobId);
                break;
            case "start":
                // set job.stopped = 0
                // set task.restart = 1 where task.status in ('SCHEMA_DOING', 'DATA_DOING', 'VERIFICATION_DOING', 'SCHEMA_DONE', 'DATA_DONE', 'VERIFICATION_DONE')
                service.getJobService().startJob(jobId);
                break;
            case "retry":
                // set job.stopped = 0, TODO 好像不用
                // set task.restart = 1 where task.status in ('SCHEMA_FAILED', 'DATA_FAILED', 'VERIFICATION_FAILED')
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
    public ApiRes getJobOptions(
            @RequestParam("ds_name") String dsName,
            @RequestParam("db_name") String dbName,
            @RequestParam(value = "lang", defaultValue = "zh_CN") String lang
    ) {
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
                taskTypes.put(TaskType.HIVE,  taskTypeName.getName(TaskType.HIVE, lang));
                taskTypes.put(TaskType.HIVE_MERGED_TRANS, taskTypeName.getName(TaskType.HIVE_MERGED_TRANS, lang));
                defaultTaskType = TaskType.HIVE;
                break;
            case HIVE_OSS:
                taskTypes.put(TaskType.HIVE_OSS, taskTypeName.getName(TaskType.HIVE_OSS, lang));
                taskTypes.put(TaskType.HIVE, taskTypeName.getName(TaskType.HIVE, lang));
                defaultTaskType = TaskType.HIVE_OSS;
                break;
            case HIVE_GLUE:
                taskTypes.put(TaskType.HIVE, taskTypeName.getName(TaskType.HIVE, lang));
                taskTypes.put(TaskType.HIVE_OSS, taskTypeName.getName(TaskType.HIVE_OSS, lang));
                defaultTaskType = TaskType.HIVE_OSS;
            case OSS:
                break;
            case ODPS:
                taskTypes.put(TaskType.ODPS_INSERT_OVERWRITE, taskTypeName.getName(TaskType.ODPS_INSERT_OVERWRITE, lang));
//                taskTypes.put(TaskType.ODPS_MERGED_TRANS, taskTypeName.getName(TaskType.ODPS_MERGED_TRANS, lang));
                taskTypes.put(TaskType.ODPS_OSS_TRANSFER, taskTypeName.getName(TaskType.ODPS_OSS_TRANSFER, lang));
                defaultTaskType = TaskType.ODPS_INSERT_OVERWRITE;
                break;
            case DATABRICKS:
                taskTypes.put(TaskType.DATABRICKS, taskTypeName.getName(TaskType.DATABRICKS, lang));
                taskTypes.put(TaskType.DATABRICKS_UDTF, taskTypeName.getName(TaskType.DATABRICKS_UDTF, lang));
                defaultTaskType = TaskType.DATABRICKS;
            case BIGQUERY:
                taskTypes.put(TaskType.BIGQUERY, taskTypeName.getName(TaskType.BIGQUERY, lang));
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

    @GetMapping("/listProjectSchemas")
    public ApiRes listProjectSchemas(@RequestParam("project") String project) {
        OdpsUtils odpsUtils = OdpsUtils.fromConfig(mmaConfig);
        List<String> schemas = odpsUtils.listSchemas(project);

        return ApiRes.ok("data", schemas);
    }

    @GetMapping("/{jobId}/batches")
    public ApiRes listJobBatches(@PathVariable("jobId") int jobId) {
        List<JobBatchModel> batches = service.getJobService().listJobBatches(jobId);
        return ApiRes.ok("data", batches);
    }
}
