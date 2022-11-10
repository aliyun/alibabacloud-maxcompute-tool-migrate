package com.aliyun.odps.mma.api;

import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.constant.TaskTypeName;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TaskLog;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.query.TaskFilter;
import com.aliyun.odps.mma.service.JobService;
import com.aliyun.odps.mma.service.PartitionService;
import com.aliyun.odps.mma.service.TaskService;
import com.aliyun.odps.mma.task.TaskManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

@RestController
@RequestMapping("/api/tasks")
public class TaskApi {
    private final TaskService taskService;
    private final JobService jobService;
    private final PartitionService ptService;
    private final TaskManager taskManager;

    @Autowired
    public TaskApi(TaskService taskService, JobService jobService, PartitionService ptService, TaskManager taskManager) {
        this.taskService = taskService;
        this.jobService = jobService;
        this.ptService = ptService;
        this.taskManager = taskManager;
    }

    @PutMapping("")
    public ApiRes getTasks(@RequestBody TaskFilter filter) {
        String tableName = filter.getTableName();
        String ptValue = filter.getPartition();

        // 如果要根据partition搜任务，必须先指定table，因为直接在partition表里执行'like 分区值'太慢了
        if (Objects.nonNull(tableName) && Objects.nonNull(ptValue)) {
            List<Integer> taskIds = taskService.getTaskIdsByPt(tableName, ptValue);
            filter.setTaskIds(taskIds);
        }

        int taskCount = taskService.getTasksCount(filter);
        List<TaskModel> tasks = taskService.getTasks(filter);
        Map<Integer, String> jobIdToDec = jobService.getIdToDecOfJobs();

        for (TaskModel task: tasks) {
            task.setJobName(jobIdToDec.get(task.getJobId()));
        }

        ApiRes apiRes = ApiRes.ok();
        apiRes.addData("data", tasks);
        apiRes.addData("total", taskCount);

        return apiRes;
    }

    @GetMapping("/{taskId}")
    public ApiRes getTaskLogs(@PathVariable("taskId") int taskId) {
        List<TaskLog> taskLogs = taskService.getTaskLogs(taskId);

        return ApiRes.ok("data", taskLogs);
    }

    @GetMapping("/{taskId}/partitions")
    public ApiRes getTaskPartitions(@PathVariable("taskId") int taskId) {
        List<PartitionModel> ptList = ptService.getPartitionsOfTask(taskId);
        return ApiRes.ok("data", ptList);
    }

    @PutMapping("/{taskId}")
    public ApiRes updateTask(@PathVariable("taskId") int taskId,  @RequestParam("action") String action) {
        action = action.toLowerCase();

        switch (action) {
            case "stop":
                taskManager.cancelTask(taskId);
                break;
            case "restart":
                taskService.restart(taskId);
                break;
            case "reset":
                taskService.updateTaskStatus(taskId, TaskStatus.INIT);
                break;
        }

        return ApiRes.ok();
    }

    @GetMapping("/{taskId}/logs")
    public ResponseEntity<String> downloadTaskLogs(@PathVariable("taskId") int taskId) {
        List<TaskLog> taskLogs = taskService.getTaskLogs(taskId);

        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        StringBuilder sb = new StringBuilder();
        for (TaskLog log : taskLogs) {
            sb
                    .append("时间: ").append(sf.format(log.getCreateTime())).append("\n")
                    .append("状态: ").append(log.getStatus()).append("\n")
                    .append("动作: ").append(log.getAction()).append("\n")
                    .append("结果: ").append(log.getMsg()).append("\n\n");
        }

        String disposition = String.format("attachment; filename=\"task_%d_log.txt\"", taskId);
        return ResponseEntity
                .ok()
                .contentType(MediaType.TEXT_PLAIN)
                .header("Content-Disposition", disposition)
                .body(sb.toString());
    }

    @GetMapping("/typeNames")
    public ApiRes getTypeNames() {
        return ApiRes.ok("data", TaskTypeName.getNameMap());
    }
}
