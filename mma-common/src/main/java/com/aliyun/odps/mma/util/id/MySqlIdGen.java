package com.aliyun.odps.mma.util.id;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class MySqlIdGen implements IdGenerator {
    @Autowired
    DataSource dataSource;

    @Override
    public Long nextId(String name) throws IdGenException {
        try(Connection conn = dataSource.getConnection()) {
            try {
                conn.setAutoCommit(false);
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                try (PreparedStatement stat1 = conn.prepareStatement("update id_gen set id = id + 1 where name=(?)")) {
                    stat1.setString(1, name);
                    stat1.execute();
                }

                try (PreparedStatement stat2 = conn.prepareStatement("select id from id_gen where name=(?)")) {
                    stat2.setString(1, name);
                    ResultSet rs = stat2.executeQuery();
                    rs.next();
                    long id = rs.getLong(1) - 1;
                    conn.commit();
                    return id;
                }
            } catch (SQLException e) {
                conn.rollback();
                throw new IdGenException(e);
            } finally {
                conn.setAutoCommit(true);
            }

        } catch (SQLException e) {
            throw new IdGenException(e);
        }
    }
}
