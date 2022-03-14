package com.aliyun.odps.mma.validation;

import com.aliyun.odps.Column;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.List;

@Resolve("*->bigint,string")
public class OdpsTableHasherUDTF extends UDTF {
    private TableHasher tableHasher = new TableHasher();
    private List<Column> columns;
    private long recordCount = 0;

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {
        super.setup(ctx);
    }

    // objects为
    // 1 "[partitioned column1, ...., column1, column2]"
    // 2 partition column1,...column1, column2....
    @Override
    public void process(Object[] objects) {
        if (tableHasher.getAdapter() == null) {
            // 1. 获取columns, 包括partition columns
            String columnsStr = (String) objects[0];
            List<OdpsColumn> odpsColumns = OdpsColumn.fromJsonArray(columnsStr);
            columns = OdpsColumn.toColumns(odpsColumns);

            // 2. 创建odps field adapter
            assert columns.size() == objects.length - 1;
            OdpsFieldAdapter fieldAdapter = new OdpsFieldAdapter(columns);

            tableHasher.setAdapter(fieldAdapter);
        }

        int VALUE_OFFSET = 1;
        for (int i = 0, n = columns.size(); i < n; i ++) {
            Column column = columns.get(i);
            Object fieldValue = objects[VALUE_OFFSET + i];
            // 3. 调用TableHasher添加列数据，完成hash序列化
            tableHasher.addColumnData(column.getName(), fieldValue);
        }

        tableHasher.update();
        recordCount += 1;

        if (recordCount % 1000 == 0) {
            System.out.printf("has processed %d records\n", recordCount);
            Util.printMem();
        }
    }

    @Override
    public void close() throws UDFException {
        forward(recordCount, tableHasher.getTableHash());
        super.close();
    }
}

class Util {
    static void printMem() {
        long u = 1000000;
        long free = Runtime.getRuntime().freeMemory() / u;
        long total = Runtime.getRuntime().totalMemory() / u;
        long max = Runtime.getRuntime().maxMemory() / u;

        System.out.println("\t Free Memory \t Total Memory \t Max Memory");
        System.out.printf("\t %sM \t %sM \t %sM\n", free, total, max);
    }
}