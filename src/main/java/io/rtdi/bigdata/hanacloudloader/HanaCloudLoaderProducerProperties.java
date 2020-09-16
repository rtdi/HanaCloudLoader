package io.rtdi.bigdata.hanacloudloader;

import java.io.File;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class HanaCloudLoaderProducerProperties extends ProducerProperties {

	public HanaCloudLoaderProducerProperties(String name) throws PropertiesException {
		super(name);
	}

	public HanaCloudLoaderProducerProperties(File dir, String name) throws PropertiesException {
		super(dir, name);
	}

}
