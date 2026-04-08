package com.aliyun.odps.mma.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface Setter {
    void bind(PreparedStatement ps) throws SQLException;
}
