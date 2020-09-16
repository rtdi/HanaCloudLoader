package io.rtdi.bigdata.hanacloudloader;

import java.sql.Connection;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;

public abstract class HanaRootTableStatement {

	protected HanaRootTable writer;

	public HanaRootTableStatement(HanaRootTable writer) {
		this.writer = writer;
	}

	protected abstract RowType getRowType();

	public HanaRootTable getWriter() {
		return writer;
	}

	protected abstract void execute(JexlRecord record, Connection conn) throws ConnectorCallerException;
	
}
