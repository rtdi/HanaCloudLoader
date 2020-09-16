package io.rtdi.bigdata.hanacloudloader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;

public class HanaWriterUpsert extends HanaRootTableStatement {
	private Map<String, String> childtablesqls;
	private Map<String, String> childtabledeletesqls;
	private String upsertsql;
	
	public HanaWriterUpsert(HanaRootTable writer) throws ConnectorCallerException {
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
		
		upsertsql = sqltext.toString();
		createChildSQLs();
	}
	
	@Override
	protected void execute(JexlRecord record, Connection conn) throws ConnectorCallerException {
		if (record != null) {
			deletechilddata(record, conn);
			upsertdata(record, conn);
			loadchilddata(record, conn, writer);
		}
	}
	
	private void loadchilddata(JexlRecord record, Connection conn, HanaTable recordwriter) throws ConnectorCallerException {
		if (recordwriter.getArrayFields() != null) {
			for (Field field : recordwriter.getArrayFields().keySet()) {
				HanaTable fieldwriter = recordwriter.getChildTable(field);
				HanaArrayColumn arraycolumn = recordwriter.getArrayFields().get(field);
				String schemaname = arraycolumn.getSchemaName();
				String sql = childtablesqls.get(schemaname);
				@SuppressWarnings("unchecked")
				List<JexlRecord> l = (List<JexlRecord>) arraycolumn.getValue(record);
				if (l != null && l.size() != 0) {
					try (PreparedStatement stmt = conn.prepareStatement(sql); ) {
						for (JexlRecord r : l) {
							int pos = 1;
							for (HanaColumn col : fieldwriter.getColumns()) {
								Object value = col.getValue(r);
								stmt.setObject(pos, value);
								pos++;
							}
							stmt.execute();
							loadchilddata(r, conn, fieldwriter);
						}
					} catch (SQLException e) {
						throw new ConnectorCallerException(
								"Cannot execute the insert statement into a child table",
								e,
								"Execute the SQL manually to validate",
								sql);
					}
				}
			}
		}
	}

	private void upsertdata(JexlRecord record, Connection conn) throws ConnectorCallerException {
		try (PreparedStatement stmt = conn.prepareStatement(upsertsql); ) {
			int pos = 1;
			for (HanaColumn col : getWriter().getColumns()) {
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
					upsertsql);
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
		return RowType.UPSERT;
	}

	private void createChildSQLs() {
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
			childtablesqls.put(name, sqltext.toString());
			
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
			childtabledeletesqls.put(name, sqltext.toString());
		}
	}
	
	@Override
	public String toString() {
		return upsertsql;
	}

}