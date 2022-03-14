package com.aliyun.odps.mma.io;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.JarURLClassLoader;
import com.aliyun.odps.mma.validation.TableHasherInter;

import java.net.URL;

public class TableHasherLoader {
    public static TableHasherInter load(TableSchema schema) {
        ClassLoader parent = TableHasherLoader.class.getClassLoader();
        URL url = parent.getResource("validation");
        JarURLClassLoader classLoader = new JarURLClassLoader(url, parent);

        try {
            Class<?> c = Class.forName("com.aliyun.odps.mma.validation.HiveTableHasher", true, classLoader);
            TableHasherInter tableHasher = (TableHasherInter) c.newInstance();
            tableHasher.initFieldAdapter(schema.getColumns(), schema.getPartitionColumns());
            return tableHasher;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        TableSchema tableSchema = new TableSchema();
        Column column = new Column("a", OdpsType.STRING);
        tableSchema.addColumn(column);
        TableHasherInter hasher = TableHasherLoader.load(tableSchema);
        System.out.println(hasher.getClass().getName());
    }
}
