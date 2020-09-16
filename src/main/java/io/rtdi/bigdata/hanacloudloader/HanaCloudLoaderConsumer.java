package io.rtdi.bigdata.hanacloudloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.rtdi.bigdata.connector.connectorframework.Consumer;
import io.rtdi.bigdata.connector.connectorframework.controller.ConsumerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;

public class HanaCloudLoaderConsumer extends Consumer<HanaCloudLoaderConnectionProperties, HanaCloudLoaderConsumerProperties> {
	Connection conn = null;
	private Map<Integer, HanaRootTable> schemawriters = new HashMap<>();

	public HanaCloudLoaderConsumer(ConsumerInstanceController instance) throws IOException {
		super(instance);
		conn = HanaCloudLoaderFactory.getDatabaseConnection((HanaCloudLoaderConnectionProperties) this.getConnectionProperties());
	}

	@Override
	public void process(TopicName topic, long offset, int partition, JexlRecord keyRecord, JexlRecord valueRecord) throws IOException {
		RowType rowtype = ValueSchema.getChangeType(valueRecord);
		if (rowtype == null) {
			rowtype = RowType.UPSERT;
		}
		HanaRootTable writer = getWriter(valueRecord);
		writer.writeRecord(valueRecord, rowtype, conn);
	}

	@Override
	protected void closeImpl() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("Cannot close the Hana connection", e);
			}
		}
	}

	@Override
	public void fetchBatchStart() throws IOException {
	}

	@Override
	public void fetchBatchEnd() throws IOException {
	}

	@Override
	public void flushDataImpl() throws IOException {
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new ConnectorCallerException("Failed to commit the data in Hana", e, null, null);
		}
	}
	
	private HanaRootTable getWriter(JexlRecord record) throws ConnectorCallerException {
		HanaRootTable w = schemawriters.get(record.getSchemaId());
		if (w == null) {
			w = new HanaRootTable(this, record.getSchema());
			schemawriters.put(record.getSchemaId(), w);
		}
		return w;
	}

}
