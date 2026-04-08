package com.aliyun.odps.mma.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class IntSetter implements Setter {
    private final int[] params;

    public IntSetter(int... params) {
        this.params = params;
    }

    @Override
    public void bind(PreparedStatement ps) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setInt(i + 1, params[i]);
        }
    }
}
