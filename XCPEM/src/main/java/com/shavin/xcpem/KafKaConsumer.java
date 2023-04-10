package com.shavin.xcpem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.yugabyte.ysql.YBClusterAwareDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Service
public class KafKaConsumer {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafKaConsumer.class);
	YBClusterAwareDataSource ds = new YBClusterAwareDataSource();
	Connection conn;

	public KafKaConsumer() throws SQLException {
		super();
		System.out.println("Kafka consumer....");
		connectToYugabyte();
		createDatabase(conn);

	}

	private void connectToYugabyte() throws SQLException {
		ds.setUrl("jdbc:yugabytedb://" + AppConstants.HOST + ":" + AppConstants.PORT + "/yugabyte");
		ds.setUser(AppConstants.DBUSER);
		ds.setPassword(AppConstants.DBPASSWORD);

		String sslMode = AppConstants.SSLMODE;
		if (!sslMode.isEmpty() && !sslMode.equalsIgnoreCase("disable")) {
			ds.setSsl(true);
			ds.setSslMode(sslMode);

			if (!AppConstants.SSLROOTCERT.isEmpty())
				ds.setSslRootCert(AppConstants.SSLROOTCERT);
		}
		try {
			if(conn!=null)
				conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn = ds.getConnection();
		System.out.println(">>>> Successfully connected to YugabyteDB!");
	}

	@KafkaListener(topics = AppConstants.TOPIC_NAME, groupId = AppConstants.GROUP_ID)
	public void consume(String message) throws SQLException  {

		try {
			save(conn, message);
			LOGGER.info(String.format("Message received -> %s", message));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			connectToYugabyte();
			
			
		}
		
	}

	private static void createDatabase(Connection conn) throws SQLException {
		Statement stmt = conn.createStatement();

		stmt.execute("DROP TABLE IF EXISTS " + AppConstants.TABLE_NAME);

		stmt.execute("CREATE TABLE " + AppConstants.TABLE_NAME + "(" + "id SERIAL PRIMARY KEY," + "message varchar,"
				+ "received timestamp DEFAULT NOW()" + ")");

		System.out.println(">>>> Successfully created " + AppConstants.TABLE_NAME + " table.");
	}

	private static void save(Connection conn, String msg) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.execute("INSERT INTO " + AppConstants.TABLE_NAME + "(message) VALUES " + "('" + msg + "')");
		stmt.close();
	}
}
