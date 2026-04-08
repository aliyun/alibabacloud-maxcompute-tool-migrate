package com.aliyun.odps.mma.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultBuilder<T> {
    T build(ResultSet rs) throws SQLException;
}
