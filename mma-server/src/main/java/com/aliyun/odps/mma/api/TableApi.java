package com.aliyun.odps.mma.api;

import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.constant.JobType;
import com.aliyun.odps.mma.execption.JobSubmittingException;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.orm.JobProxy;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.query.TableFilter;
import com.aliyun.odps.mma.service.DataSourceService;
import com.aliyun.odps.mma.service.PartitionService;
import com.aliyun.odps.mma.service.TableService;
import com.aliyun.odps.mma.validator.ValidateJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

@RestController
@RequestMapping("/api/tables")
@Validated
public class TableApi {
    private final DataSourceService dsService;
    private final TableService ts;
    private final PartitionService ps;
    private final OrmFactory proxyFactory;

    @Autowired
    public TableApi(DataSourceService dsService, TableService ts, PartitionService ps, OrmFactory proxyFactory) {
        this.dsService = dsService;
        this.ts = ts;
        this.ps = ps;
        this.proxyFactory = proxyFactory;
    }

    @PutMapping("")
    public ApiRes getTables(@RequestBody TableFilter tableFilter) {
        int tablesCount = ts.getTablesCount(tableFilter);
        List<TableModel> tables =  ts.getTables(tableFilter);
        List<Integer> tableIds = new ArrayList<>(tables.size());

        Map<Integer, TableModel> tableMap = new HashMap<>();
        for (TableModel tm: tables) {
            tableIds.add(tm.getId());
            tableMap.put(tm.getId(), tm);
        }

        List<Map<String, Object>> ptStatList = ps.ptStatOfTables(tableIds);

        for (Map<String, Object> ptStat: ptStatList) {
            Integer tableId = (Integer) ptStat.get("tableId");
            TableModel table = tableMap.get(tableId);
            int count = ((Long)ptStat.get("count")).intValue();
            table.setPartitions(table.getPartitions() + count);

            switch ((String) ptStat.get("status")) {
                case "INIT":
                    break;
                case "DOING":
                    table.setPartitionsDoing(count);
                    break;
                case "DONE":
                    table.setPartitionsDone(count);
                    break;
                case "FAILED":
                    table.setPartitionsFailed(count);
                    break;
            }
        }

        ApiRes apiRes = ApiRes.ok();
        apiRes.addData("total", tablesCount);
        apiRes.addData("data", tables);

        return apiRes;
    }

    @GetMapping("/{tableId}")
    public TableModel getTableById(@PathVariable("tableId") int tableId) {
        TableModel table = ts.getTableById(tableId);

        if (Objects.isNull(table)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        return table;
    }

    @PutMapping("/{tableId}/job")
    public ResponseEntity<ApiRes> submitSingleTableJob(
            @PathVariable("tableId") int tableId,
            @ValidateJob @RequestBody JobModel jobModel
    ) throws JobSubmittingException {
        TableModel table = this.getTableById(tableId);

        if (Objects.isNull(table)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        JobProxy job = proxyFactory.newJobProxy(jobModel);

        // 补全job信息，这里的信息只用来做记录
        DataSourceModel ds = dsService.getDataSource(table.getSourceId()).get();
        jobModel.setSourceName(ds.getName());
        jobModel.setDbName(table.getDbName());
        jobModel.setType(JobType.Tables);


        Integer jobId = job.submitTable(table);
        return new ResponseEntity<>(ApiRes.ok("job_id", jobId), HttpStatus.OK);
    }
 }
