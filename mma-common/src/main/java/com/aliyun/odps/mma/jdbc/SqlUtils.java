package com.aliyun.odps.mma.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * biliang.wbl
 */
public class SqlUtils {
    public static final int MAX_BIND_PARENT_SIZE = 1000;
    public static final int MAX_BIND_PK_SIZE = 10000;
    public static final int FETCH_SIZE = 10000;
    public static final Setter NONE_SETTER = new IntSetter();

    public static void startTransaction(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    public static void execute(Connection conn, String sql, Setter setter) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        setter.bind(ps);
        ps.execute();
    }

    public static <T> T query(Connection conn, String sql, Setter setter, ResultBuilder<T> builder) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        setter.bind(ps);
        ResultSet rs = ps.executeQuery();
        rs.setFetchDirection(ResultSet.FETCH_FORWARD);
        rs.setFetchSize(FETCH_SIZE);
        return builder.build(rs);
    }

    public static Set<String> query4Set(Connection conn, String sql, Setter setter) throws SQLException {
        return query(conn, sql, setter, new ResultBuilder<Set<String>>() {
            @Override
            public Set<String> build(ResultSet rs) throws SQLException {
                Set<String> set = new HashSet<>();
                while (rs.next()) {
                    set.add(rs.getString(1));
                }
                return set;
            }
        });
    }

    public static List<String> query4List(Connection conn, String sql, Setter setter) throws SQLException {
        return query(conn, sql, setter, new ResultBuilder<List<String>>() {
            @Override
            public List<String> build(ResultSet rs) throws SQLException {
                List<String> list = new ArrayList<>();
                while (rs.next()) {
                    list.add(rs.getString(1));
                }
                return list;
            }
        });
    }

    public static long query4Long(Connection conn, String sql, Setter setter, long defValue) throws SQLException {
        return query(conn, sql, setter, new ResultBuilder<Long>() {
            @Override
            public Long build(ResultSet rs) throws SQLException {
                if (rs.next()) {
                    return rs.getLong(1);
                }
                return defValue;
            }
        });
    }

    public static int query4Int(Connection conn, String sql, Setter setter, int defValue) throws SQLException {
        return query(conn, sql, setter, new ResultBuilder<Integer>() {
            @Override
            public Integer build(ResultSet rs) throws SQLException {
                if (rs.next()) {
                    return rs.getInt(1);
                }
                return defValue;
            }
        });
    }

    public static String query4String(Connection conn, String sql, Setter setter) throws SQLException {
        return query(conn, sql, setter, new ResultBuilder<String>() {
            @Override
            public String build(ResultSet rs) throws SQLException {
                if (rs.next()) {
                    return rs.getString(1);
                }
                return null;
            }
        });
    }

    public static int executeUpdate(Connection conn, String sql, Setter setter) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        setter.bind(ps);
        return ps.executeUpdate();
    }

    public static int[] executeBatch(Connection conn, String sql, List<Setter> setters) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(sql);
        for (Setter s : setters) {
            s.bind(ps);
            ps.addBatch();
        }
        return ps.executeBatch();
    }
}
