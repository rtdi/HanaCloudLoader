package io.rtdi.bigdata.hanacloudloader;

import io.rtdi.bigdata.connector.properties.ConnectionProperties;

public class HanaCloudLoaderConnectionProperties extends ConnectionProperties {

	private static final String ENDPOINT = "hanacloud.endpoint";
	private static final String USERNAME = "hanacloud.username";
	private static final String PASSWORD = "hanacloud.password";
	private static final String TARGETSCHEMA = "hanacloud.targetschema";

	public HanaCloudLoaderConnectionProperties(String name) {
		super(name);
		properties.addStringProperty(ENDPOINT, "Endpoint (Host)", "The Hana Cloud Endpoint", "sap-icon://target-group", "<id>.hana.hanacloud.ondemand.com:443", true);
		properties.addStringProperty(USERNAME, "Username", "Hana database username", "sap-icon://target-group", null, true);
		properties.addPasswordProperty(PASSWORD, "Password", "Password", "sap-icon://target-group", null, true);
		properties.addStringProperty(TARGETSCHEMA, "Target database schema", "Database schema the target tables should be created in", "sap-icon://target-group", null, false);
	}

	public String getJDBCURL() {
		String endpoint = getEndpoint();
		return "jdbc:sap://" + endpoint + "/?encrypt=true&validateCertificate=false";
	}
	
	public String getEndpoint() {
		String endpoint = properties.getStringPropertyValue(ENDPOINT);
		if (!endpoint.contains(":")) {
			endpoint = endpoint + ":443";
		}
		return endpoint;
	}
	
	public String getUsername() {
		return properties.getStringPropertyValue(USERNAME);
	}
	
	public String getPassword() {
		return properties.getPasswordPropertyValue(PASSWORD);
	}
	
	public String getTargetSchema() {
		String schema = properties.getStringPropertyValue(TARGETSCHEMA);
		if (schema == null) {
			return getUsername();
		} else {
			return schema;
		}
	}
}
