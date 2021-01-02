package io.rtdi.bigdata.hanacloudloader;

import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;

public class HanaRootTable extends HanaTable {
	private Map<String, HanaTable> allchildtables;
	private Map<RowType, HanaRootTableStatement> writerstatements = new HashMap<>();


	public HanaRootTable(HanaCloudLoaderConsumer consumer, Schema schema) throws ConnectorCallerException {
		super(consumer, schema, null);
	}
	
	@Override
	protected HanaRootTable getRootTable() {
		return this;
	}

	public Map<String, HanaTable> getAllChildTables() {
		return allchildtables;
	}
	
	public HanaTable getChildTableFromDictionary(String schemaname) {
		if (allchildtables == null) {
			return null;
		} else {
			return allchildtables.get(schemaname);
		}
	}
	
	public void addChildTableToDictionary(String schemaname, HanaTable hanaWriter) {
		if (allchildtables == null) {
			allchildtables = new HashMap<>();
		}
		allchildtables.put(schemaname, hanaWriter);
	}
	
	protected void writeRecord(JexlRecord record, RowType rowtype, Connection conn) throws ConnectorCallerException {
		HanaRootTableStatement writer = writerstatements.get(rowtype);
		if (writer == null) {
			switch (rowtype) {
			case DELETE:
			case EXTERMINATE:
				writer = new HanaWriterDelete(this, conn);
				break;
			case TRUNCATE:
				writer = new HanaWriterTruncate(this);
				break;
			case REPLACE:
			case UPDATE:
			case INSERT:
			case UPSERT:
				writer = new HanaWriterUpsert(this, conn);
				break;
			}
			writerstatements.put(rowtype, writer);
		}
		writer.execute(record, conn);
	}

	public void executeBatch() throws IOException {
		for (HanaRootTableStatement writer : writerstatements.values()) {
			writer.executeBatch();
		}
	}

}