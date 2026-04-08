package com.aliyun.odps.mma.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StringResultBuilder implements ResultBuilder<String> {
    private final String defaultValue;

    public StringResultBuilder(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String build(ResultSet rs) throws SQLException {
        if (rs.next()) {
            return rs.getString(1);
        } else {
            return defaultValue;
        }
    }
}
