package io.rtdi.bigdata.hanacloudloader;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;

public class HanaColumnPATH extends HanaColumnPK {

	public static final String NAME = "__RECORD_PATH";

	public HanaColumnPATH() {
		super(NAME, "NVARCHAR(5000)");
	}

	@Override
	public Object getValue(JexlRecord record) throws ConnectorCallerException {
		return record.getPath();
	}
}
