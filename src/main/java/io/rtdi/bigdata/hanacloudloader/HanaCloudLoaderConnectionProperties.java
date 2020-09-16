package io.rtdi.bigdata.hanacloudloader;

import io.rtdi.bigdata.connector.properties.ConnectionProperties;

public class HanaCloudLoaderConnectionProperties extends ConnectionProperties {

	private static final String JDBCURL = "s4hana.jdbcurl";
	private static final String USERNAME = "s4hana.username";
	private static final String PASSWORD = "s4hana.password";
	private static final String TARGETSCHEMA = "s4hana.targetschema";

	public HanaCloudLoaderConnectionProperties(String name) {
		super(name);
		properties.addStringProperty(JDBCURL, "JDBC URL", "The JDBC URL to use for connecting to the Hana system", "sap-icon://target-group", "jdbc:sap://localhost:3xx15/yy", true);
		properties.addStringProperty(USERNAME, "Username", "Hana database username", "sap-icon://target-group", null, true);
		properties.addPasswordProperty(PASSWORD, "Password", "Password", "sap-icon://target-group", null, true);
		properties.addStringProperty(TARGETSCHEMA, "Target database schema", "Database schema the target tables should be created in", "sap-icon://target-group", null, true);
	}

	public String getJDBCURL() {
		return properties.getStringPropertyValue(JDBCURL);
	}
	
	public String getUsername() {
		return properties.getStringPropertyValue(USERNAME);
	}
	
	public String getPassword() {
		return properties.getPasswordPropertyValue(PASSWORD);
	}
	
	public String getTargetSchema() {
		return properties.getStringPropertyValue(TARGETSCHEMA);
	}
}
