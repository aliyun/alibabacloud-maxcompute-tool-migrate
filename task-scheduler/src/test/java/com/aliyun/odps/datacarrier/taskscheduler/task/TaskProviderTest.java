//package com.aliyun.odps.datacarrier.taskscheduler.task;
//
//import java.util.LinkedList;
//
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.aliyun.odps.datacarrier.taskscheduler.DataSource;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.TableMigrationConfig;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaConfigUtils;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
//import com.aliyun.odps.datacarrier.taskscheduler.MockHiveMetaSource;
//import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
//import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.TableMetaModel;
//import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImpl;
//import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProvider;
//
//public class TaskProviderTest {
//  private static TaskProvider taskProvider;
//
//  @BeforeClass
//  public static void setup() throws MmaException {
//    MetaSource mockMetaSource = new MockHiveMetaSource();
//    MmaMetaManagerDbImpl mmaMetaManager =
//        new MmaMetaManagerDbImpl(null, mockMetaSource, false);
//    taskProvider = new TaskProvider(mmaMetaManager);
//  }
//
//  @Test
//  public static void testGetTasks() {
//
//  }
//
//  @Test
//  public static void testGenerateHiveToOdpsNonPartitionedTableMigrationTask() {
//    TableMigrationConfig tableMigrationConfig = new TableMigrationConfig();
//    TableMetaModel tableMetaModel = new TableMetaModel();
//    tableMetaModel.databaseName = "test_db";
//    tableMetaModel.tableName = "test_tbl";
//    tableMetaModel.partitionColumns = new LinkedList<>();
//
//    taskProvider.generateNonPartitionedTableMigrationTask(DataSource.Hive, tableMetaModel, )
//  }
//}
