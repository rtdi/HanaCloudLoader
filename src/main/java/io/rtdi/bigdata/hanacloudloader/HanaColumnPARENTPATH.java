package io.rtdi.bigdata.hanacloudloader;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class HanaColumnPARENTPATH extends HanaColumn {

	public static final String NAME = "__PARENT_RECORD_PATH";

	public HanaColumnPARENTPATH() {
		super(NAME, "NVARCHAR(5000)");
	}

	@Override
	public Object getValue(JexlRecord record) throws ConnectorCallerException {
		return record.getParent().getPath();

	}
}
