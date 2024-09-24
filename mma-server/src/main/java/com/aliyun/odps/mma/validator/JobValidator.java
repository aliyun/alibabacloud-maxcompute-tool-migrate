package com.aliyun.odps.mma.validator;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mma.api.ApiRes;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.config.PartitionFilter;
import com.aliyun.odps.mma.constant.JobType;
import com.aliyun.odps.mma.model.*;
import com.aliyun.odps.mma.service.Service;
import com.aliyun.odps.mma.service.TableService;
import com.aliyun.odps.mma.util.ListUtils;
import com.aliyun.odps.mma.util.OdpsUtils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class JobValidator implements ConstraintValidator<ValidateJob, JobModel> {
    private static final Logger logger = LoggerFactory.getLogger(JobValidator.class);
    private final MMAConfig mmaConfig;
    private boolean configRequired;
    private final Service service;
    private ValidateJob jobConstraint;
    private Map<String, Field> fieldOfJob = new HashMap<>();

    @Autowired
    public JobValidator(MMAConfig config, Service service) {
        this.mmaConfig = config;
        this.service = service;
    }

    @Override
    public void initialize(ValidateJob constraintAnnotation) {
        this.configRequired = constraintAnnotation.configRequired().equals("true");

        this.jobConstraint = constraintAnnotation;
        //this.fieldOfJob = JobModel.JobModelInfo.fieldMap;
        //Class<JobConfig> c = JobConfig.class;
        Class<JobModel> c = JobModel.class;
        for (Field field : c.getDeclaredFields()) {
            field.setAccessible(true);
            fieldOfJob.put(field.getName(), field);
        }
    }

    @Override
    public boolean isValid(JobModel job, ConstraintValidatorContext context) {
        context.disableDefaultConstraintViolation();

        String defaultMessage = this.jobConstraint.message();
        ApiRes apiRes = new ApiRes();
        apiRes.setMessage(defaultMessage);

        boolean ok = isValid(job, apiRes);
        context.buildConstraintViolationWithTemplate(apiRes.toJson()).addConstraintViolation();
        return ok;
    }

    private boolean isValid(JobModel job, ApiRes apiRes) {
        JobConfig config = job.getConfig();

        if (this.configRequired && Objects.isNull(config)) {
            apiRes.setMessage("there no necessary config");

            return !this.configRequired;
        }

        String[] requiredFields = this.jobConstraint.require();
        // 校验必填项
        if (Arrays.asList(requiredFields).contains("type")) {
            if (Objects.isNull(job.getType())) {
                apiRes.addError(
                        "type",
                        String.format(
                                "type cannot be empty, should be one of (%s)",
                                Arrays.stream(JobType.values()).map(Objects::toString).collect(Collectors.joining(", "))
                        )
                );

                return false;
            }
        }

        for (String requiredField: requiredFields) {
            if (requiredField.equals("type")) {
                continue;
            }

            Field fieldReflect = fieldOfJob.get(requiredField);
            if (Objects.isNull(fieldReflect)) {
                String fieldName = fieldFormat(requiredField);
                apiRes.addError(fieldName, String.format("%s is required", fieldName));
                return false;
            }

            try {
                Object value = fieldReflect.get(job);

                if (Objects.isNull(value)) {
                    String fieldName = fieldFormat(requiredField);
                    apiRes.addError(fieldName, String.format("%s is required", fieldName));
                    return false;
                }

                if (value instanceof List) {
                    if (((List<?>) value).isEmpty()) {
                        String fieldName = fieldFormat(requiredField);

                        apiRes.addError(fieldName, String.format("%s is required, cannot be empty", fieldName));
                        return false;
                    }
                }

                if (value instanceof Map) {
                    String fieldName = fieldFormat(requiredField);
                    if (((Map<?, ?>) value).isEmpty()) {
                        apiRes.addError(fieldName, String.format("%s is required, cannot be empty", fieldName));
                        return false;
                    }
                }
            } catch (IllegalAccessException e) {
                return false;
            }
        }

        boolean ok = true;

        switch (job.getType()) {
            case Database:
                ok = verifyDBJob(job, apiRes);
                break;
            case Tables:
                ok = verifyTablesJob(job, apiRes);
                break;
            case Partitions:
                ok = verifyPartitionsJob(job, apiRes);
                break;
        }

        // 校验odps project
        String projectName = job.getDstOdpsProject();

        if (Objects.isNull(projectName)) {
            return ok;
        }

        try {
            OdpsUtils odpsUtils = OdpsUtils.fromConfig(this.mmaConfig);
            boolean exists = odpsUtils.isProjectExists(projectName);
            if (!exists) {
                apiRes.addError("odps_project", String.format("%s is not exists", projectName));
            }

            return ok && exists;
        } catch (OdpsException e) {
            logger.warn("job validation failed:", e);
            apiRes.setMessage(String.format("cannot connect to odps, %s", e.getMessage()));
            return false;
        }
    }

    private boolean verifyDBJob(JobModel job, ApiRes apiRes) {
        if (! getDatasource(job, apiRes).isPresent()) {
            return false;
        }

        boolean ok = verifyDatabase(job, apiRes);
        ok = ok && verifyTableBlackOrWhiteList(job.getConfig(), apiRes);
        ok = ok && verifyPartitionFilters(job.getConfig(), apiRes);

        return ok;
    }

    private boolean verifyTablesJob(JobModel job, ApiRes apiRes) {
        if (! getDatasource(job, apiRes).isPresent()) {
            return false;
        }

        if (! verifyDatabase(job, apiRes)) {
            return false;
        }

        boolean ok = true;

        JobConfig config = job.getConfig();

        // 校验必填项
        List<String> tableNames = config.getTables();
        if (Objects.isNull(tableNames)) {
            ok = false;
            apiRes.addError("tables", "cannot be empty");
        }

        // 校验tables名字
        TableService ts = service.getTableService();
        List<TableModel> tables = ts.getTablesOfDbByNames(job.getSourceName(), job.getDbName(), tableNames);

        if (tables.size() < tableNames.size()) {
            Set<String> real = new HashSet<>(tableNames.size());
            Set<String> given = new HashSet<>(tableNames);
            tables.forEach(t -> real.add(t.getName()));
            given.removeAll(real);
            ok = false;
            apiRes.addError("tables", String.format("tables %s are not existed", given));
        }

        // 校验分区过滤
        ok = ok && verifyPartitionFilters(config, apiRes);

        return ok;
    }

    private boolean verifyPartitionsJob(JobModel job, ApiRes apiRes) {
        Optional<DataSourceModel> dsOpt = getDatasource(job, apiRes);

        if (! dsOpt.isPresent()) {
            return false;
        }

        JobConfig config = job.getConfig();
        List<Integer> partitionIds = config.getPartitions();
        if (ListUtils.size(partitionIds) == 0) {
            apiRes.addError("partitions", "cannot be empty");
            return false;
        }

        List<PartitionModel> partitions = this.service
                .getPartitionService()
                .getPartitions(partitionIds);

        if (partitions.size() != partitionIds.size()) {
            Set<Integer> real = new HashSet<>(partitions.size());
            Set<Integer> given = new HashSet<>(partitionIds);
            partitions.forEach(p -> real.add(p.getId()));
            given.removeAll(real);

            apiRes.addError("partitions",  String.format("partitions %s are not existed", given));
            return false;
        }

        Set<Integer> dataSources = partitions.stream().map(PartitionModel::getSourceId).collect(Collectors.toSet());
        if (dataSources.size() > 1) {
            apiRes.addError("partitions", "partitions should only be from single data source");
            return false;
        }

        if (! dataSources.contains(dsOpt.get().getId())) {
            apiRes.addError("partitions", "the datasource of partitions is not the " + dsOpt.get().getName());
            return false;
        }

        return true;
    }

    private Optional<DataSourceModel> getDatasource(JobModel job, ApiRes apiRes) {
        String dsName = job.getSourceName();
        if (Objects.isNull(dsName)) {
            apiRes.addError("datasource", "cannot be empty");
            return Optional.empty();
        }

        Optional<DataSourceModel> ds = service.getDsService().getDataSource(dsName);
        if (! ds.isPresent()) {
            apiRes.addError("datasource", String.format("%s is not existed", dsName));
            return Optional.empty();
        }

        return ds;
    }

    private boolean verifyDatabase(JobModel job, ApiRes apiRes) {
        String dbName = job.getDbName();
        if (Objects.isNull(dbName)) {
            apiRes.addError("db_name", "cannot be empty");
            return false;
        }

        Optional<DataBaseModel> dbOpt = this.service.getDbService().getDbByName(job.getSourceName(), dbName);
        if (! dbOpt.isPresent()) {
            apiRes.addError("db_name", String.format("%s is not existed", dbName));
            return false;
        }

        return true;
    }

    private boolean verifyPartitionFilters(JobConfig config, ApiRes apiRes) {
        boolean ok = true;

        List<PartitionFilter> filters = config.getPartitionFilters();
        for (PartitionFilter filter: filters) {
            String errMsg = filter.getSyntaxErr();
            if (Objects.nonNull(errMsg)) {
                ok = false;
                apiRes.addError(
                        "partition_filters",
                        String.format("the expression \"%s\" has syntax error: %s", filter.getFilterExpr(), errMsg)
                );
            }
        }

        return ok;
    }

    private boolean verifyTableBlackOrWhiteList(JobConfig config, ApiRes apiRes) {
        // 校验黑白名单
        List<String> tableBlackList = config.getTableBlackList();
        List<String> tableWhiteList = config.getTableWhiteList();

        if (ListUtils.size(tableBlackList) > 0 && ListUtils.size(tableWhiteList) > 0) {
            apiRes.addError("table_black_list", "conflict with table_white_list");
            apiRes.addError("table_white_list", "conflict with table_black_list");
            return false;
        }

        return true;
    }

    private String fieldFormat(String field) {
        StringBuilder sb = new StringBuilder();

        for(char c: field.toCharArray()) {
            if (Character.isUpperCase(c)) {
                sb.append("_").append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }
}
