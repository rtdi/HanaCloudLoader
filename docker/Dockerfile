FROM rtdi/connectorrootapp

LABEL maintainer="info@rtdi.io"

ADD https://raw.githubusercontent.com/rtdi/HanaCloudLoader/master/docker/global.properties conf/rtdiconfig/HanaCloudLoader/
ADD https://github.com/rtdi/HanaCloudLoader/releases/latest/download/hanacloudloader.war ./webapps/

EXPOSE 8080
EXPOSE 8443


CMD ["appcontainer.sh"]
