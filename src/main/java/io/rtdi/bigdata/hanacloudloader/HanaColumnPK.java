package io.rtdi.bigdata.hanacloudloader;

import org.apache.avro.Schema.Field;

public class HanaColumnPK extends HanaColumn {

	public HanaColumnPK(Field f) {
		super(f, null);
	}

	public HanaColumnPK(String name, String hanadatatype) {
		super(name, hanadatatype);
	}

}
