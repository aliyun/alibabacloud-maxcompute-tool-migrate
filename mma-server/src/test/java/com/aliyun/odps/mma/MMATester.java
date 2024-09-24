package com.aliyun.odps.mma;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.BearerTokenAccount;
import com.aliyun.odps.mma.config.HiveGlueConfig;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.config.HiveConfig;
import com.aliyun.odps.mma.constant.TaskStatus;
import com.aliyun.odps.mma.mapper.JobMapper;
import com.aliyun.odps.mma.mapper.PartitionMapper;
import com.aliyun.odps.mma.mapper.TableMapper;
import com.aliyun.odps.mma.mapper.TaskMapper;
import com.aliyun.odps.mma.meta.*;
import com.aliyun.odps.mma.model.TaskModel;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.mma.service.DbService;
import com.aliyun.odps.mma.service.JobService;
import com.aliyun.odps.mma.service.TableService;
import com.aliyun.odps.mma.service.TaskService;
import com.aliyun.odps.mma.sql.OdpsSqlUtils;
import com.aliyun.odps.mma.sql.PartitionValue;
//import com.aliyun.odps.mma.task.HiveTaskExecutor;
import com.aliyun.odps.mma.task.TaskManager;
import com.aliyun.odps.mma.task.TaskUtils;
import com.aliyun.odps.mma.util.I18nUtils;
import com.aliyun.odps.mma.util.OdpsUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;


@SpringBootApplication
public class MMATester implements CommandLineRunner {
    @Autowired
    DataSourceMetaLoader dw;

    @Autowired
    MMAConfig config;

    @Autowired
    DbService dbService;

    @Autowired
    TableService tableService;

    @Autowired
    TaskService taskService;

    @Autowired
    TaskMapper taskMapper;

    @Autowired
    JobService jobService;

    @Autowired
    JobMapper jobMapper;

    @Autowired
    PartitionMapper partitionMapper;

    @Autowired
    ApplicationContext appCtx;

    @Autowired
    TaskUtils taskUtil;

    @Autowired
    TaskManager taskManager;

    @Autowired
    TableMapper tableMapper;

    @Autowired
    OrmFactory proxyFactory;

    @Autowired
    DSManager dsManager;

    @Autowired
    PartitionMapper pm;

//    @Autowired
//    HiveTaskExecutor he;

//    @Autowired
//    HiveGlueConfig hiveGlueConfig;

    @Autowired
    I18nUtils i18nUtils;


    public static void main(String[] args) {
        CmdArgParser parser = new CmdArgParser();
        parser.parse(args);

        SpringApplicationBuilder builder = new SpringApplicationBuilder(MMATester.class);
        builder.web(WebApplicationType.NONE);
        SpringApplication app = builder.build();

        ConfigurableApplicationContext ctx = app.run(args);

        if (ctx.isRunning()) {
            SpringApplication.exit(ctx, () -> 0);
        }

        System.exit(0);
    }

    @Override
    public void run(String... args) throws Exception {
        testJobServe();
    }

    public void testI18nUtils() {
        System.out.println(i18nUtils.get("meta.timer", "zh_CN", ""));
    }

//    public void testHiveGlueConfig() {
//        hiveGlueConfig.setSourceName("hive_glue1");
//        System.out.println(hiveGlueConfig.getTableDataPathFromS3("s3://datahousedev01/dw_dim/test"));
//    }
//
//    private void testTaskProxy() {
//        TaskModel taskModel = taskMapper.getTaskById(1);
//        TaskProxy task = proxyFactory.newTaskProxy(taskModel);
//        List<PartitionValue> values = task.getDstOdpsPartitionValues();
//
//        for (PartitionValue value: values) {
//            System.out.println(value.transfer((name, type, v) -> String.format("%s=%s", name, v), ","));
//        }
//    }

//    private void testNoPtSql() throws Exception {
//        TaskModel taskModel = taskMapper.getTaskById(32778);
//        TaskProxy task = proxyFactory.newTaskProxy(taskModel);
//        String sql = OdpsSqlUtils.createTableSql(
//                task.getOdpsProjectName(),
//                task.getOdpsSchemaName(),
//                task.getOdpsTableName(),
//                task.getOdpsTableSchema(),
//                null,
//                15,
//                null,
//                null
//        );
//
//        System.out.println(sql);
//
//        String addPtSql = OdpsSqlUtils.addPartitionsSql(
//                task.getOdpsTableFullName(),
//                task.getDstOdpsPartitionValues()
//        );
//
//        System.out.println(addPtSql);
//
//        System.out.println(OdpsSqlUtils.selectCountSql(
//                task.getOdpsTableFullName(),
//                task.getSrcPartitionValues()
//        ));
//
//
//        he.setTask(taskModel);
//        he._initTask();
//        System.out.println(he.getUDTFSql());
//
//        System.out.println(he.getCountSql());
//    }
//
//    private void testMergePtSql() throws Exception {
//        TaskModel taskModel = taskMapper.getTaskById(32801);
//        TaskProxy task = proxyFactory.newTaskProxy(taskModel);
//        String sql = OdpsSqlUtils.createTableSql(
//                task.getOdpsProjectName(),
//                task.getOdpsSchemaName(),
//                task.getOdpsTableName(),
//                task.getOdpsTableSchema(),
//                null,
//                15,
//                null,
//                null
//        );
//
//        System.out.println(sql);
//
//        String addPtSql = OdpsSqlUtils.addPartitionsSql(
//                task.getOdpsTableFullName(),
//                task.getDstOdpsPartitionValues()
//        );
//
//        System.out.println(addPtSql);
//
//        System.out.println(OdpsSqlUtils.selectCountSql(
//                task.getOdpsTableFullName(),
//                task.getSrcPartitionValues()
//        ));
//
//
//        he.setTask(taskModel);
//        he._initTask();
//        System.out.println(he.getUDTFSql());
//
//        System.out.println(he.getCountSql());
//    }

    private void testOdpsSql() {
        TaskModel taskModel = taskMapper.getTaskById(32538);
        TaskProxy task = proxyFactory.newTaskProxy(taskModel);
        List<PartitionValue> values = task.getSrcPartitionValues();
        List<PartitionValue> mergedValues = task.getDstOdpsPartitionValues();

    }

//    private void testHiveExecutor() throws Exception {
//        TaskModel taskModel = taskMapper.getTaskById(32538);
//        //TaskModel taskModel = taskMapper.getTaskById(114);
//        he.setTask(taskModel);
//        he._initTask();
//
//        System.out.println(he.getUDTFSql());
//    }

    private void testBearerToken() throws OdpsException {
        OdpsUtils odpsUtils = new OdpsUtils(
                config.getConfig(MMAConfig.MC_AUTH_ACCESS_ID),
                config.getConfig(MMAConfig.MC_AUTH_ACCESS_KEY),
                config.getConfig(MMAConfig.MC_ENDPOINT),
                config.getConfig(MMAConfig.MC_DEFAULT_PROJECT)
        );

        String bearerToken = odpsUtils.getBearerToken("mma_test_hz", "", "acid_persons");
        System.out.println(bearerToken);

        Account account = new BearerTokenAccount(bearerToken);
        Odps odps = new Odps(account);
        odps.setEndpoint(config.getConfig(MMAConfig.MC_ENDPOINT));
        odps.setDefaultProject("mma_test_hz");

        Table table = odps.tables().get("acid_persons");
        table.reload();
        System.out.println(table.getRecordNum());
    }

    private void testTaskMapper() {
        TaskModel taskModel = taskMapper.getTaskById(1);
        taskModel.setStatus(TaskStatus.DATA_FAILED);
        taskMapper.updateTaskStatus(taskModel);
    }

    private void testJobServe() {
        jobService.startJob(62);
    }
}

