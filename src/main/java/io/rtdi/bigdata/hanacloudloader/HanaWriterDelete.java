package io.rtdi.bigdata.hanacloudloader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;

public class HanaWriterDelete extends HanaRootTableStatement {
	private Map<String, String> childtabledeletesqls;
	private String deletesql;
	
	public HanaWriterDelete(HanaRootTable writer) throws ConnectorCallerException {
		super(writer);
		StringBuffer sqltext = new StringBuffer();
		StringBuffer where = new StringBuffer();
		sqltext.append("delete from ");
		sqltext.append(getWriter().getTableIdentifier());
		sqltext.append(" where ");
		for (HanaColumnPK pk : writer.getPrimaryKeys()) {
			if (where.length() != 0) {
				where.append(" AND ");
			}
			where.append("\"").append(pk.getColumnName()).append("\" = ?");
		}
		sqltext.append(where);

		deletesql = sqltext.toString();
		createChildSQLs();
	}
	
	@Override
	protected void execute(JexlRecord record, Connection conn) throws ConnectorCallerException {
		if (record != null) {
			deletechilddata(record, conn);
			deletedata(record, conn);
		}
	}
	
	private void deletedata(JexlRecord record, Connection conn) throws ConnectorCallerException {
		try (PreparedStatement stmt = conn.prepareStatement(deletesql); ) {
			int pos = 1;
			for (HanaColumnPK col : writer.getPrimaryKeys()) {
				Object value = col.getValue(record);
				stmt.setObject(pos, value);
				pos++;
			}
			stmt.execute();
		} catch (SQLException e) {
			throw new ConnectorCallerException(
					"Cannot execute the delete statement",
					e,
					"Execute the SQL manually to validate",
					deletesql);
		}
	}

	private void deletechilddata(JexlRecord record, Connection conn) throws ConnectorCallerException {
		for (String sql : childtabledeletesqls.values()) {
			try (PreparedStatement stmt = conn.prepareStatement(sql); ) {
				int pos = 1;
				for (HanaColumn col : writer.getPrimaryKeys()) {
					Object value = col.getValue(record);
					stmt.setObject(pos, value);
					pos++;
				}
				stmt.execute();
			} catch (SQLException e) {
				throw new ConnectorCallerException(
						"Cannot execute the upsert statement",
						e,
						"Execute the SQL manually to validate",
						sql);
			}
		}
	}

	@Override
	protected RowType getRowType() {
		return RowType.DELETE;
	}

	private void createChildSQLs() {
		childtabledeletesqls = new HashMap<>();
		for (String name : writer.getAllChildTables().keySet()) {
			HanaTable w = writer.getAllChildTables().get(name);
			StringBuffer sqltext = new StringBuffer();
			StringBuffer projection = new StringBuffer();
			sqltext.append("delete from ");
			sqltext.append(w.getTableIdentifier());
			sqltext.append(" where ");
			for (HanaColumnPK pk : w.getPrimaryKeys()) {
				if (projection.length() != 0) {
					projection.append(" AND ");
				}
				projection.append("\"").append(pk.getColumnName()).append("\" = ?");
			}
			sqltext.append(projection);
			childtabledeletesqls.put(name, sqltext.toString());
		}
	}
	
	@Override
	public String toString() {
		return deletesql;
	}

}