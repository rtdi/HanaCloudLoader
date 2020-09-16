package io.rtdi.bigdata.hanacloudloader;

import java.sql.Connection;
import java.sql.SQLException;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;

public abstract class HanaWriterStatement {
	protected HanaTable writer;
	
	public HanaWriterStatement(HanaTable writer) {
		super();
		this.writer = writer;
	}

	protected abstract void execute(Connection conn) throws SQLException, ConnectorCallerException;
	
	public HanaTable getWriter() {
		return writer;
	}

}