package io.rtdi.bigdata.hanacloudloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.ConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryConsumer;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class HanaCloudLoaderFactory extends ConnectorFactory<HanaCloudLoaderConnectionProperties>
implements IConnectorFactoryConsumer<HanaCloudLoaderConnectionProperties, HanaCloudLoaderConsumerProperties>{

	public HanaCloudLoaderFactory() {
		super("HanaCloudLoader");
	}

	@Override
	public Consumer<HanaCloudLoaderConnectionProperties, HanaCloudLoaderConsumerProperties> createConsumer(ConsumerInstanceController instance) throws IOException {
		return new HanaCloudLoaderConsumer(instance);
	}

	@Override
	public HanaCloudLoaderConnectionProperties createConnectionProperties(String name) throws PropertiesException {
		return new HanaCloudLoaderConnectionProperties(name);
	}

	@Override
	public HanaCloudLoaderConsumerProperties createConsumerProperties(String name) throws PropertiesException {
		return new HanaCloudLoaderConsumerProperties(name);
	}

	@Override
	public BrowsingService<HanaCloudLoaderConnectionProperties> createBrowsingService(ConnectionController controller) throws IOException {
		return new HanaCloudLoaderBrowse(controller);
	}

	@Override
	public boolean supportsBrowsing() {
		return false;
	}

	static Connection getDatabaseConnection(HanaCloudLoaderConnectionProperties props) throws ConnectorCallerException {
		try {
			return getDatabaseConnection(props.getJDBCURL(), props.getUsername(), props.getPassword());
		} catch (SQLException e) {
			throw new ConnectorCallerException("Failed to establish a database connection", e, null, props.getJDBCURL());
		}
	}
	
	static Connection getDatabaseConnection(String jdbcurl, String user, String passwd) throws SQLException {
        try {
            Class.forName("com.sap.db.jdbc.Driver");
            Connection conn = DriverManager.getConnection(jdbcurl, user, passwd);
            conn.setAutoCommit(false);
            return conn;
        } catch (ClassNotFoundException e) {
            throw new SQLException("No Hana JDBC driver library found");
        }
	}

}
