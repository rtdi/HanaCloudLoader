package io.rtdi.bigdata.hanacloudloader;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDate;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroType;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.IAvroDatatype;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;

public class HanaColumn {

	private Field field;
	private String columnname;
	private String hanadatatype;
	private IAvroDatatype avrotype;
	private List<String> path;

	public HanaColumn(Field f, List<String> path) {
		this.field = f;
		String name = AvroField.getOriginalName(field);
		if (path != null && path.size() != 0) {
			StringBuffer b = new StringBuffer();
			for ( String p : path) {
				b.append(p);
				b.append('_');
			}
			b.append(name);
			this.columnname = b.toString(); 
		} else {
			this.columnname = name;
		}
		avrotype = AvroType.getAvroDataType(f.schema());
		this.hanadatatype = getHanaDatatype(avrotype);
		this.path = path;
	}

	public HanaColumn(String name, String hanadatatype) {
		this.columnname = name;
		this.hanadatatype = hanadatatype;
	}

	public String getColumnName() {
		return columnname;
	}

	public String getHanaDataTypeString() {
		return hanadatatype;
	}

	private static String getHanaDatatype(IAvroDatatype avrotype) {
		switch (avrotype.getAvroType()) {
		case AVROANYPRIMITIVE:
			return "NVARCHAR(5000)";
		case AVROARRAY:
			return "NVARCHAR(5000)";
		case AVROSTRING:
			return "NVARCHAR(5000)";
		case AVROBYTE:
			return "TINYINT";
		default:
			return avrotype.toString();
		}
	}

	public Field getField() {
		return field;
	}

	public Object getValue(JexlRecord record) throws ConnectorCallerException {
		if (path != null && path.size() != 0) {
			for ( String p : path) {
				record = (JexlRecord) record.get(p);
			}
		}
		if (record == null) {
			return null;
		} else {
			return getHanaNativeValue(record.get(field.name()));
		}
	}

	protected Object getHanaNativeValue(Object value) throws ConnectorCallerException {
		if (value == null) {
			return null;
		}
		try {
			switch (avrotype.getAvroType()) {
			case AVROANYPRIMITIVE:
				break;
			case AVROARRAY:
				break;
			case AVROBOOLEAN:
				break;
			case AVROBYTE:
				break;
			case AVROBYTES:
				break;
			case AVROCLOB:
				break;
			case AVRODATE: {
				Instant date = ((AvroDate) avrotype).convertToJava(value);
				return Date.from(date);
			}
			case AVRODECIMAL:
				break;
			case AVRODOUBLE:
				break;
			case AVROENUM:
				break;
			case AVROFIXED:
				break;
			case AVROFLOAT:
				break;
			case AVROINT:
				break;
			case AVROLONG:
				break;
			case AVROMAP:
				break;
			case AVRONCLOB:
				break;
			case AVRONVARCHAR:
				break;
			case AVRORECORD:
				break;
			case AVROSHORT:
				break;
			case AVROSTGEOMETRY:
				break;
			case AVROSTPOINT:
				break;
			case AVROSTRING:
				break;
			case AVROTIMEMICROS:
				break;
			case AVROTIMEMILLIS:
				break;
			case AVROTIMESTAMPMICROS:
				break;
			case AVROTIMESTAMPMILLIS: {
				Instant utc = ((AvroTimestamp) avrotype).convertToJava(value);
				Timestamp ts = Timestamp.from(utc);
				return ts;
			}
			case AVROURI:
				break;
			case AVROUUID:
				break;
			case AVROVARCHAR:
				break;
			default:
				break;
			}
			return avrotype.convertToJava(value);
		} catch (PipelineCallerException e) {
			throw new ConnectorCallerException(e.getMessage(), e.getCause(), e.getHint(), e.getCausingObject());
		}
	}
	
	@Override
	public String toString() {
		return columnname;
	}

}
