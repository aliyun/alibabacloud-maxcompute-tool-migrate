package com.aliyun.odps.mma.validation;

import com.aliyun.odps.Column;

import java.util.List;

public interface TableHasherInter {
   void initFieldAdapter(List<Column> columns, List<Column> partitionColumns);
   String getTableHash();
   void addColumnData(Object ...objects);
   void update();
}
