package io.rtdi.bigdata.hanacloudloader;

import java.util.List;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

public class HanaArrayColumn extends HanaColumn {

	private String schemaname;

	public HanaArrayColumn(Field f, List<String> path) {
		super(f, path);
		this.schemaname = IOUtils.getBaseSchema(f.schema()).getElementType().getFullName();
	}

	public String getSchemaName() {
		return schemaname;
	}

}
