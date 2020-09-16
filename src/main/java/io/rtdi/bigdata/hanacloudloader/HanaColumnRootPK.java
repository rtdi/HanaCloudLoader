package io.rtdi.bigdata.hanacloudloader;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.IAvroNested;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;

public class HanaColumnRootPK extends HanaColumnPK {

	private String pkcolumnname;

	public HanaColumnRootPK(Field f) {
		super(f);
		this.pkcolumnname = "__" + AvroField.getOriginalName(f);
	}

	@Override
	public String getColumnName() {
		return pkcolumnname;
	}

	public Object getValue(JexlRecord record) throws ConnectorCallerException {
		JexlRecord root = getRoot(record);
		return getHanaNativeValue(root.get(getField().name()));
	}

	private static JexlRecord getRoot(JexlRecord record) {
		IAvroNested root = record;
		while (root.getParent() != null) {
			root = root.getParent();
		}
		return (JexlRecord) root;
	}
}
