package io.rtdi.bigdata.hanacloudloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.entity.TableEntry;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;

public class HanaCloudLoaderBrowse extends BrowsingService<HanaCloudLoaderConnectionProperties> {
	
	private Connection conn;

	public HanaCloudLoaderBrowse(ConnectionController controller) throws IOException {
		super(controller);
	}

	@Override
	public void open() throws IOException {
		conn = HanaCloudLoaderFactory.getDatabaseConnection(getConnectionProperties());
	}

	@Override
	public void close() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
			}
			conn = null;
		}
	}

	@Override
	public List<TableEntry> getRemoteSchemaNames() throws IOException {
		return null;
	}

	@Override
	public Schema getRemoteSchemaOrFail(String name) throws IOException {
		throw new ConnectorRuntimeException("This connector does not have remote schemas", null, null, null);
	}

	public Connection getConnection() {
		return conn;
	}

	@Override
	public void validate() throws IOException {
		close();
		open();
		String sql = "select top 1 table_name from tables t";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return;
			} else {
				return;
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Reading the Hana data dictionary failed", e, 
					"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
		} finally {
			close();
		}
		
	}

	@Override
	public void deleteRemoteSchemaOrFail(String remotename) throws IOException {
		throw new ConnectorRuntimeException("This connector does not have remote schemas", null, null, null);
	}
}
