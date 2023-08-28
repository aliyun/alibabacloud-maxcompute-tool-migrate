package com.aliyun.odps.mma.meta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;

import com.aliyun.odps.mma.config.HiveConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveUtils {
    Logger logger = LoggerFactory.getLogger(HiveUtils.class);

    private HiveConfig config;

    public HiveUtils(HiveConfig config) {
        this.config = config;

        if (this.config.getBoolean(HiveConfig.HIVE_METASTORE_SASL_ENABLED)) {
            String gssJaasFile = this.config.getConfig(HiveConfig.JAVA_SECURITY_AUTH_LOGIN_CONFIG);
            String krb5File = this.config.getConfig(HiveConfig.JAVA_SECURITY_KRB5_CONF);

            System.setProperty("java.security.auth.login.config", gssJaasFile);
            System.setProperty("java.security.krb5.conf", krb5File);
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        }

        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void executeQuery(String sql, String taskName, Map<String, String> sqlSettings, SqlResultFunc func) throws SQLException {
        executeQuery(sql, taskName, sqlSettings, null, func);
    }

    public void executeQuery(
            String sql, String taskName, Map<String, String> sqlSettings,
            ConnGetterFunc connGetterFunc, SqlResultFunc func
    ) throws SQLException {
        String hiveJdbcUrl = config.getConfig(HiveConfig.HIVE_JDBC_URL);
        String user = config.getConfig(HiveConfig.HIVE_JDBC_USERNAME);
        String password = config.getConfig(HiveConfig.HIVE_JDBC_PASSWORD);

        DriverManager.setLoginTimeout(600);

        try (Connection conn = DriverManager.getConnection(hiveJdbcUrl, user, password)) {
            if (Objects.nonNull(connGetterFunc)) {
                connGetterFunc.call(conn);
            }

            String notSetMrNameStr = System.getProperty("NOT_SET_MR_JOB_NAME");
            boolean notSetMrName = Objects.equals(notSetMrNameStr, "true");

            try (Statement stmt = conn.createStatement()) {
                if(! notSetMrName) {
                    stmt.execute("SET mapreduce.job.name=" + taskName);
                }

                for (Map.Entry<String, String> entry : sqlSettings.entrySet()) {
                    stmt.execute("SET " + entry.getKey() + "=" + entry.getValue());
                }

                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        func.call(rs);
                    }
                }
            }
        }
    }

    @FunctionalInterface
    public interface SqlResultFunc {
        void call(ResultSet rs) throws SQLException;
    }

    @FunctionalInterface
    public interface ConnGetterFunc {
        void call(Connection conn) throws SQLException;
    }
}
