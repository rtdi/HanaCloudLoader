package io.rtdi.bigdata.hanacloudloader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.kafka.avro.RowType;

public class HanaWriterUpsert extends HanaRootTableStatement {
	private Map<String, PreparedStatement> childtablesqls;
	private Map<String, PreparedStatement> childtabledeletesqls;
	private PreparedStatement upsertsql;
	
	public HanaWriterUpsert(HanaRootTable writer, Connection conn) throws ConnectorCallerException {
		super(writer);
		StringBuffer sqltext = new StringBuffer();
		StringBuffer sqlparamlist = new StringBuffer();
		StringBuffer projection = new StringBuffer();
		sqltext.append("upsert ");
		sqltext.append(getWriter().getTableIdentifier());
		sqltext.append(" (");
		for (HanaColumn col : writer.getColumns()) {
			if (projection.length() != 0) {
				projection.append(", ");
				sqlparamlist.append(", ");
			}
			projection.append("\"").append(col.getColumnName()).append("\"");
			sqlparamlist.append("?");
		}
		sqltext.append(projection);
		sqltext.append(") values (");
		sqltext.append(sqlparamlist);
		sqltext.append(")");
		sqltext.append(" with primary key");
		
		try {
			upsertsql = conn.prepareStatement(sqltext.toString());
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
			deletechilddata(record);
			upsertdata(record);
			loadchilddata(record, writer);
		}
	}
	
	private void loadchilddata(JexlRecord record, HanaTable recordwriter) throws ConnectorCallerException {
		if (recordwriter.getArrayFields() != null) {
			for (Field field : recordwriter.getArrayFields().keySet()) {
				HanaTable fieldwriter = recordwriter.getChildTable(field);
				HanaArrayColumn arraycolumn = recordwriter.getArrayFields().get(field);
				String schemaname = arraycolumn.getSchemaName();
				PreparedStatement stmt = childtablesqls.get(schemaname);
				@SuppressWarnings("unchecked")
				List<JexlRecord> l = (List<JexlRecord>) arraycolumn.getValue(record);
				if (l != null && l.size() != 0) {
					try {
						for (JexlRecord r : l) {
							int pos = 1;
							for (HanaColumn col : fieldwriter.getColumns()) {
								Object value = col.getValue(r);
								stmt.setObject(pos, value);
								pos++;
							}
							stmt.addBatch();
							loadchilddata(r, fieldwriter);
						}
					} catch (SQLException e) {
						throw new ConnectorCallerException(
								"Cannot execute the insert statement into a child table",
								e,
								"Execute the SQL manually to validate",
								stmt.toString());
					}
				}
			}
		}
	}

	private void upsertdata(JexlRecord record) throws ConnectorCallerException {
		try {
			int pos = 1;
			for (HanaColumn col : getWriter().getColumns()) {
				Object value = col.getValue(record);
				upsertsql.setObject(pos, value);
				pos++;
			}
			upsertsql.addBatch();
		} catch (SQLException e) {
			throw new ConnectorCallerException(
					"Cannot execute the upsert statement",
					e,
					"Execute the SQL manually to validate",
					upsertsql.toString());
		}
	}

	private void deletechilddata(JexlRecord record) throws ConnectorCallerException {
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
		return RowType.UPSERT;
	}

	private void createChildSQLs(Connection conn) throws SQLException {
		childtablesqls = new HashMap<>();
		childtabledeletesqls = new HashMap<>();
		for (String name : writer.getAllChildTables().keySet()) {
			HanaTable w = writer.getAllChildTables().get(name);
			StringBuffer sqltext = new StringBuffer();
			StringBuffer sqlparamlist = new StringBuffer();
			StringBuffer projection = new StringBuffer();
			sqltext.append("insert into ");
			sqltext.append(w.getTableIdentifier());
			sqltext.append(" (");
			for (HanaColumn col : w.getColumns()) {
				if (projection.length() != 0) {
					projection.append(", ");
					sqlparamlist.append(", ");
				}
				projection.append("\"").append(col.getColumnName()).append("\"");
				sqlparamlist.append("?");
			}
			sqltext.append(projection);
			sqltext.append(") values (");
			sqltext.append(sqlparamlist);
			sqltext.append(")");
			childtablesqls.put(name, conn.prepareStatement(sqltext.toString()));
			
			sqltext = new StringBuffer();
			projection = new StringBuffer();
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
		return "upsert";
	}

	@Override
	protected void executeBatch() throws IOException {
		try {
			upsertsql.executeBatch();
			for (PreparedStatement stmt : childtabledeletesqls.values()) {
				stmt.executeBatch();
			}
			for (PreparedStatement stmt : childtablesqls.values()) {
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