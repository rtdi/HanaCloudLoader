package io.rtdi.bigdata.hanacloudloader;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.IAvroNested;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;

public class HanaColumnFIELD extends HanaColumn {

	public static final String NAME = "__FIELD";
	public HanaColumnFIELD() {
		super(NAME, "NVARCHAR(256)");
	}

	@Override
	public Object getValue(JexlRecord record) throws ConnectorCallerException {
		IAvroNested d = record.getParent();
		if (d != null && d.getParentField() != null) {
			return AvroField.getOriginalName(d.getParentField());
		} else {
			return null;
		}
	}
	
}
