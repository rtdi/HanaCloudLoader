package io.rtdi.bigdata.hanacloudloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RowType;

public class HanaWriterDelete extends HanaRootTableStatement {
	private Map<String, PreparedStatement> childtabledeletesqls;
	private PreparedStatement deletesql;
	
	public HanaWriterDelete(HanaRootTable writer, Connection conn) throws ConnectorCallerException {
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

		try {
			deletesql = conn.prepareStatement(sqltext.toString());
			createChildSQLs(conn);
		} catch (SQLException e) {
			throw new ConnectorCallerException(
					"Cannot create the SQL statements",
					e,
					"Execute the SQL manually to validate",
					null);
		}
	}
	
	@Override
	protected void execute(JexlRecord record, Connection conn) throws ConnectorCallerException {
		if (record != null) {
			deletechilddata(record, conn);
			deletedata(record, conn);
		}
	}
	
	private void deletedata(JexlRecord record, Connection conn) throws ConnectorCallerException {
		try {
			int pos = 1;
			for (HanaColumnPK col : writer.getPrimaryKeys()) {
				Object value = col.getValue(record);
				deletesql.setObject(pos, value);
				pos++;
			}
			deletesql.addBatch();
		} catch (SQLException e) {
			throw new ConnectorCallerException(
					"Cannot execute the delete statement",
					e,
					"Execute the SQL manually to validate",
					deletesql.toString());
		}
	}

	private void deletechilddata(JexlRecord record, Connection conn) throws ConnectorCallerException {
		for (PreparedStatement stmt : childtabledeletesqls.values()) {
			try {
				int pos = 1;
				for (HanaColumn col : writer.getPrimaryKeys()) {
					Object value = col.getValue(record);
					stmt.setObject(pos, value);
					pos++;
				}
				stmt.addBatch();
			} catch (SQLException e) {
				throw new ConnectorCallerException(
						"Cannot execute the upsert statement",
						e,
						"Execute the SQL manually to validate",
						stmt.toString());
			}
		}
	}

	@Override
	protected RowType getRowType() {
		return RowType.DELETE;
	}

	private void createChildSQLs(Connection conn) throws SQLException {
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
			childtabledeletesqls.put(name, conn.prepareStatement(sqltext.toString()));
		}
	}
	
	@Override
	public String toString() {
		return "delete";
	}

	@Override
	protected void executeBatch() throws IOException {
		try {
			deletesql.executeBatch();
			for (PreparedStatement stmt : childtabledeletesqls.values()) {
				stmt.executeBatch();
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException(
					"Failed when running the executeBatch() command to update the target tables",
					e,
					null,
					null);
		}
	}

}