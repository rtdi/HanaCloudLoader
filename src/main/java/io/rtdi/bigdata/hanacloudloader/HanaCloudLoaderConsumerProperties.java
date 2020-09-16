package io.rtdi.bigdata.hanacloudloader;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

public class HanaCloudLoaderConsumerProperties extends ConsumerProperties {

	public HanaCloudLoaderConsumerProperties(String name) throws PropertiesException {
		super(name);
	}

	public HanaCloudLoaderConsumerProperties(String name, TopicName topic) throws PropertiesException {
		super(name, topic);
	}

	public HanaCloudLoaderConsumerProperties(String name, String pattern) throws PropertiesException {
		super(name, pattern);
	}

}
