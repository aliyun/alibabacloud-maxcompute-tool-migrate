package com.aliyun.odps.mma.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LongSetter implements Setter {
    private final long[] params;

    public LongSetter(long... params) {
        this.params = params;
    }

    @Override
    public void bind(PreparedStatement ps) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setLong(i + 1, params[i]);
        }
    }
}
