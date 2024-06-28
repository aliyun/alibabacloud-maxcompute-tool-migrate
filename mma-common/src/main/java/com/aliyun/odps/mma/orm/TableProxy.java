package com.aliyun.odps.mma.orm;

import java.util.List;
import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.service.PartitionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TableProxy {
    private final PartitionService partitionService;
    private TableModel tableModel;

    @Autowired
    public TableProxy(PartitionService partitionService) {
        this.partitionService = partitionService;
    }

    protected void init(TableModel tableModel) {
        this.tableModel = tableModel;
    }

    public List<PartitionModel> getPartitions() {
        return partitionService.getPartitionsOfTable(tableModel.getId());
    }

    public int partitionsNum() {
        return partitionService.getPartitionsNumOfTable(tableModel.getId());
    }

    public boolean isPartitionedTable() {
        return tableModel.isHasPartitions();
    }

    public String getDbName() {
        return tableModel.getDbName();
    }

    public String getSchemaName() {
        return tableModel.getSchemaName();
    }

    public String getName() {
        return tableModel.getName();
    }

    public TableModel getTableModel() {
        return tableModel;
    }

    public MMATableSchema getTableSchema() {
        return this.tableModel.getSchema();
    }

    public boolean hasDecimalColumn() {
        return this.tableModel.hasDecimalColumn();
    }

    public boolean decimalOdps2() {
        return this.tableModel.decimalOdps2();
    }

    public Integer getLifeCycle() {
        return this.tableModel.getLifecycle();
    }

    public String getFullname() {
        return String.format("%s.%s.%s", tableModel.getSourceId(), tableModel.getDbName(), tableModel.getName());
    }
}
