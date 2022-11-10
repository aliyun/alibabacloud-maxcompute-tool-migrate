package com.aliyun.odps.mma;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Component
public class DBInitializer {
	DataSource dataSource;
	@Value("classpath:/db/migration/V1.0__init_db.sql")
	private Resource sqlResource;

	public DBInitializer(@Autowired DataSource dataSource) {
		this.dataSource = dataSource;
	}

	@PostConstruct
	public void init() throws SQLException, IOException {
		try (Reader reader = new InputStreamReader(sqlResource.getInputStream(), StandardCharsets.UTF_8)) {
			String[] sqlArray = FileCopyUtils.copyToString(reader).trim().split(";");

			try (Connection conn = dataSource.getConnection()) {
				for (String sql: sqlArray) {
					try (Statement stmt = conn.createStatement()) {
						stmt.execute(sql);
					}
				}
			}
		}
	}
}
