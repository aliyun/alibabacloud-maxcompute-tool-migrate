package com.aliyun.odps.mma.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

public class StringSetter implements Setter {
    private final String[] params;

    public StringSetter(String... params) {
        this.params = params;
    }

    public StringSetter(Collection<String> params) {
        this.params = new String[params.size()];
        params.toArray(this.params);
    }

    @Override
    public void bind(PreparedStatement ps) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            ps.setString(i + 1, params[i]);
        }
    }
}
