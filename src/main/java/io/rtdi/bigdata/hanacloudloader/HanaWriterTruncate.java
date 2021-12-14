package io.rtdi.bigdata.hanacloudloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RowType;

public class HanaWriterTruncate extends HanaRootTableStatement {
	
	public HanaWriterTruncate(HanaRootTable writer) throws ConnectorCallerException {
		super(writer);
	}
	
	@Override
	protected void execute(JexlRecord record, Connection conn) throws ConnectorCallerException {
		if (record != null) {
			deletedata(record, conn);
		}
	}
	
	private void deletedata(JexlRecord record, Connection conn) throws ConnectorCallerException {
		Map<HanaColumn, Object> usedcolumns = new HashMap<>();
		StringBuffer where = new StringBuffer();
		for (HanaColumn col : writer.getColumns()) {
			Object value = col.getValue(record);
			if (value != null) {
				usedcolumns.put(col, value);
				if (where.length() != 0) {
					where.append(" AND ");
				}
				where.append("\"").append(col.getColumnName()).append("\" = ?");
			}
		}
		if (usedcolumns.size() != 0) {
			StringBuffer sqltext;
			StringBuffer roottablepklist = new StringBuffer();
			for (HanaColumnPK pk : writer.getPrimaryKeys()) {
				if (roottablepklist.length() != 0) {
					roottablepklist.append(", ");
				}
				roottablepklist.append("\"").append(pk.getColumnName()).append("\"");
			}
			
			for (HanaTable childtable : writer.getAllChildTables().values()) {
				sqltext = new StringBuffer();
				sqltext.append("delete from ").append(childtable.getTableIdentifier());
				sqltext.append(" where (").append(roottablepklist).append(") in ");
				
				StringBuffer childtablepklist = new StringBuffer();
				for (HanaColumnPK pk : childtable.getPrimaryKeys()) {
					if (childtablepklist.length() != 0) {
						childtablepklist.append(", ");
					}
					childtablepklist.append("\"").append(pk.getColumnName()).append("\"");
				}
				sqltext.append("( select ");
				sqltext.append(childtablepklist);
				sqltext.append(" from ").append(childtable.getTableIdentifier());
				sqltext.append(" where ");
				sqltext.append(where);
				sqltext.append(")");
				String sql = sqltext.toString();
				try (PreparedStatement stmt = conn.prepareStatement(sql); ) {
					int pos = 1;
					for (Object value : usedcolumns.values()) {
						stmt.setObject(pos, value);
						pos++;
					}
					stmt.execute();
				} catch (SQLException e) {
					throw new ConnectorCallerException(
							"Cannot execute the delete statement",
							e,
							"Execute the SQL manually to validate",
							sql);
				}

			}
			
			sqltext = new StringBuffer();
			sqltext.append("delete from ");
			sqltext.append(getWriter().getTableIdentifier());
			sqltext.append(" where ");
			sqltext.append(where);
			String sql = sqltext.toString();
			try (PreparedStatement stmt = conn.prepareStatement(sql); ) {
				int pos = 1;
				for (Object value : usedcolumns.values()) {
					stmt.setObject(pos, value);
					pos++;
				}
				stmt.execute();
			} catch (SQLException e) {
				throw new ConnectorCallerException(
						"Cannot execute the delete statement",
						e,
						"Execute the SQL manually to validate",
						sql);
			}
		}
	}


	@Override
	protected RowType getRowType() {
		return RowType.TRUNCATE;
	}
	
	@Override
	public String toString() {
		return "truncate table";
	}

	@Override
	protected void executeBatch() throws IOException {
	}

}