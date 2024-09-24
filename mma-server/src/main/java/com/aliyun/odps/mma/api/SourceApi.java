package com.aliyun.odps.mma.api;

import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.config.SourceConfigUtils;
import com.aliyun.odps.mma.constant.ActionType;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.meta.DSManager;
import com.aliyun.odps.mma.meta.MetaLoader;
import com.aliyun.odps.mma.meta.MetaLoaderUtils;
import com.aliyun.odps.mma.model.ActionLog;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.query.SourceFilter;
import com.aliyun.odps.mma.service.DataSourceService;
import com.aliyun.odps.mma.util.I18nUtils;
import com.aliyun.odps.mma.util.Result;
import com.aliyun.odps.mma.validator.JsonField;
import com.aliyun.odps.mma.validator.ValidateJson;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.server.ResponseStatusException;

import java.text.SimpleDateFormat;
import java.util.*;

@Validated
@RestController
@RequestMapping("/api/sources")
public class SourceApi {
    private static final Logger logger = LoggerFactory.getLogger(SourceApi.class);

    private SourceConfigUtils sourceConfigUtils;
    private DataSourceService ds;
    private MetaLoaderUtils metaLoaderUtils;
    private DSManager dsManager;
    private I18nUtils i18nUtils;

    @Autowired
    public SourceApi(
            DataSourceService ds,
            SourceConfigUtils sourceConfigUtils,
            MetaLoaderUtils metaLoaderUtils,
            DSManager dsManager,
            I18nUtils i18nUtils
    ) {
        this.ds = ds;
        this.sourceConfigUtils = sourceConfigUtils;
        this.metaLoaderUtils = metaLoaderUtils;
        this.dsManager = dsManager;
        this.i18nUtils = i18nUtils;
    }

    @PostMapping("")
    public ApiRes addSource(
            @ValidateJson(
                    message = "failed to add data source",
                    fields = {
                            @JsonField(key = "name", required = true),
                            @JsonField(key = "type", required = true, type = SourceType.class)
                    }
            )
            @RequestBody Map<String, Object> dsJson
    ) throws Exception {
        String errMsg = "failed to add data source";
        // get datasource model from json
        ObjectMapper om = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);

        DataSourceModel dm = om.readValue(om.writeValueAsString(dsJson), DataSourceModel.class);

        if (ds.isDataSourceExisted(dm.getName())) {
            return ApiRes.error(
                    errMsg,
                    "name",
                    String.format("%s is existed", dm.getName())
            );
        }

        SourceConfig config = sourceConfigUtils.newSourceConfig(dm.getType(), dm.getName());
        config.openMemMode();
        Map<String, String> configErrors = config.initFromMap(dsJson);
        if (!configErrors.isEmpty()) {
            return  ApiRes.error(errMsg, configErrors);
        }

        MetaLoader metaLoader = metaLoaderUtils.getMetaLoader(config.getSourceType());
        try {
            metaLoader.checkConfig(config);
        } catch (Exception e) {
            logger.error("failed to add data source {}", dm.getName(), e);
            return ApiRes.error(String.format("failed to add data source: %s", e.getMessage()), null);
        }

        // 保存数据源
        ds.insertDataSource(dm);
        dm.setConfig(config);
        config.dumpMem();

        // 加载元数据
        Result<Void, String> ret = dsManager.loadDataSourceMeta(dm.getName());
        if (ret.isErr()) {
            return new ApiRes(String.format("failed to add data source: %s", ret.getError()));
        }

        Map<String, Object> ok = new HashMap<>();

        ok.put("id", dm.getId());
        ok.put("name", dm.getName());
        return ApiRes.ok("data", ok);
    }

    @PutMapping("/{sourceId}")
    public ApiRes updateSource(
            @PathVariable("sourceId") int sourceId,
            @RequestBody Map<String, Object> dsJson
    ) {
        Optional<DataSourceModel> dmOpt = ds.getDataSource(sourceId);

        if (!dmOpt.isPresent()) {
            return new ApiRes("data source doest not exist");
        }

        String errMsg = "failed to update data source";
        DataSourceModel dm = dmOpt.get();

        Object nameObj = dsJson.get("name");
        String newName = null;
        if (Objects.nonNull(nameObj) && nameObj instanceof String) {
            String name = (String) nameObj;
            if (! name.equals(dm.getName())) {
                newName = name;
            }

            if ((! name.equals(dm.getName())) && ds.isDataSourceExisted(name)) {
                return ApiRes.error(
                        errMsg,
                        "name",
                        String.format("%s is existed", dm.getName())
                );

            }
        }

        SourceConfig config = sourceConfigUtils.newSourceConfig(dm.getType(), dm.getName());
        config.openMemMode();
        Map<String, String> configErrors = config.addConfigItems(dsJson);
        if (!configErrors.isEmpty()) {
            return ApiRes.error(errMsg, configErrors);
        }

        if (Objects.nonNull(newName)) {
            ds.updateDSName(dm.getId(), newName);
        }

        MetaLoader metaLoader = metaLoaderUtils.getMetaLoader(config.getSourceType());
        try {
            metaLoader.checkConfig(config);
        } catch (Exception e) {
            logger.error("update data source {}", dm.getName(), e);
            return new ApiRes(String.format("failed to update source: %s", e.getMessage()));
        }

        config.dumpMem();

        Map<String, Object> ok = new HashMap<>();
        ok.put("name", dm.getName());
        return ApiRes.ok(ok);
    }

    @GetMapping("")
    public ApiRes getDataSources(SourceFilter sourceFilter) {
        return  ApiRes.ok("data", ds.getDataSources(sourceFilter));
    }

    @GetMapping("/{sourceId}")
    public ApiRes getDataSource(@PathVariable("sourceId") int sourceId, @RequestParam(value = "config", required = false) Integer withConfig) {
        Optional<DataSourceModel> dmOpt = ds.getDataSource(sourceId, withConfig != null);
        if (! dmOpt.isPresent()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        return ApiRes.ok("data", dmOpt.get());
    }

    @GetMapping("/byName")
    public ApiRes getDataSource(@RequestParam("name") String sourceName, @RequestParam(value = "config", required = false) Integer withConfig) {
        Optional<DataSourceModel> dmOpt = ds.getDataSource(sourceName, withConfig != null);
        if (! dmOpt.isPresent()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        return ApiRes.ok("data", dmOpt.get());
    }

    @GetMapping("/{sourceName}/config")
    public ApiRes getDataSourceConfig(
            @PathVariable("sourceName") String sourceName,
            @RequestParam(value = "lang", defaultValue = "zh_CN") String langCode
    ) {
        Optional<DataSourceModel> dmOpt = ds.getDataSource(sourceName);
        if (! dmOpt.isPresent()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }

        DataSourceModel dm = dmOpt.get();
        SourceConfig sc = dm.getConfig();
        return ApiRes.ok("data", sc.toJsonObj(langCode));
    }

    @PutMapping("/{sourceId}/update")
    public ResponseEntity<ApiRes> updateDataSourceMeta(@PathVariable("sourceId") int sourceId) {
        Optional<DataSourceModel> dmOpt = ds.getDataSource(sourceId);
        if (! dmOpt.isPresent()) {
            return new ResponseEntity<>(new ApiRes("data source doest not existed"), HttpStatus.NOT_FOUND);
        }

        DataSourceModel dm = dmOpt.get();
        // 加载元数据
        Result<Void, String> ret = dsManager.loadDataSourceMeta(dm.getName());
        if (ret.isErr()) {
            return new ResponseEntity<>(
                    new ApiRes(String.format("failed to update data source: %s", ret.getError())),
                    HttpStatus.BAD_REQUEST
            );
        }

        return new ResponseEntity<>(ApiRes.ok(), HttpStatus.OK);
    }

    @GetMapping("/{sourceId}/progress")
    public ResponseEntity<ApiRes> getProgress(@PathVariable("sourceId") int sourceId) {
        Optional<DataSourceModel> dmOpt = ds.getDataSource(sourceId);
        if (! dmOpt.isPresent()) {
            return new ResponseEntity<>(ApiRes.error("data source doest not existed", null), HttpStatus.OK);
        }

        DataSourceModel dm = dmOpt.get();
        String error = dsManager.getError(dm.getName());

        if (Objects.nonNull(error)) {
            return new ResponseEntity<>(ApiRes.error(error, null), HttpStatus.OK);
        }

        float progress = dsManager.getProgress(dmOpt.get().getName());
        return new ResponseEntity<>(ApiRes.ok("progress", progress), HttpStatus.OK);
    }

    @PutMapping("/{sourceId}/init")
    public ResponseEntity<ApiRes> runInitializer(@PathVariable("sourceId") int sourceId) {
        Optional<DataSourceModel> dmOpt = ds.getDataSource(sourceId);
        if (! dmOpt.isPresent()) {
            return new ResponseEntity<>(ApiRes.error("data source doest not existed", null), HttpStatus.OK);
        }

        Result<Void, String> r = dsManager.runInitializer(dmOpt.get().getName());

        if (r.isOk()) {
            return new ResponseEntity<>(ApiRes.ok(), HttpStatus.OK);
        }

        return new ResponseEntity<>(new ApiRes("failed to run datasource initializer: " + r.getError()), HttpStatus.OK);
    }

    @GetMapping("/{sourceId}/action_logs")
    public ApiRes getSourceActionLog(@PathVariable("sourceId") int sourceId, @RequestParam("actionType") ActionType actionType) {
        List<ActionLog> logs = ds.getActionLogs(sourceId, actionType);

        return ApiRes.ok("data", logs);
    }

    @GetMapping("/{sourceId}/action_logs_download")
    public ResponseEntity<String> downloadActionLogs(@PathVariable("sourceId") int sourceId, @RequestParam("actionType") ActionType actionType) {
        List<ActionLog> logs = ds.getActionLogs(sourceId, actionType);

        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        StringBuilder sb = new StringBuilder();
        for (ActionLog log : logs) {
            sb
                    .append("时间: ").append(sf.format(log.getCreateTime())).append("\n")
                    .append("状态: ").append(log.getStatus()).append("\n")
                    .append("操作: ").append(log.getAction()).append("\n")
                    .append("详情: ").append(log.getMsg()).append("\n\n");
        }

        String disposition = String.format("attachment; filename=\"s%d_action_log.txt\"", sourceId);
        return ResponseEntity
                .ok()
                .contentType(MediaType.TEXT_PLAIN)
                .header("Content-Disposition", disposition)
                .body(sb.toString());
    }
    @GetMapping("/items")
    public ApiRes getItems(
            @RequestParam("type") String sourceTypeStr,
            @RequestParam(value = "lang", defaultValue = "zh_CN") String langCode
    ) {
        SourceType sourceType = SourceType.valueOf(sourceTypeStr);
        SourceConfig config = sourceConfigUtils.newSourceConfig(sourceType, "");
        List<Map<String, Object>> _items = config.toJsonObj(langCode);

        Map<String, Object> name = new HashMap<>();
        name.put("key", "name");
        name.put("desc", i18nUtils.get("source.name", langCode, "数据源名"));
        name.put("type", "string");
        name.put("required", true);

        Map<String, Object> type = new HashMap<>();
        type.put("key", "type");
        type.put("desc", i18nUtils.get("source.type", langCode, "数据源类型"));
        type.put("type", "string");
        type.put("required", true);

        List<Map<String, Object>> items = new ArrayList<>();
        items.add(name);
        items.add(type);
        items.addAll(_items);

        return ApiRes.ok("data", items);
    }

    @GetMapping("/types")
    public ApiRes getSourceTypes() {
        SourceType[] types = SourceType.values();

        return ApiRes.ok("data", types);
    }
}
