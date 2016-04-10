#!/usr/bin/env bash
export SPARK_LOCAL_IP=`ip addr list ${SPARK_NETWORK_INTERFACE} | grep "inet " | cut -d" " -f6 | cut -d"/" -f1`
export SPARK_MASTER_IP=`getent hosts ${SPARK_MASTER} | awk '{ print $1 }'`
export CASSANDRA_IP=`getent hosts ${CASSANDRA} | awk '{ print $1 }'`

${SPARK_HOME}/bin/spark-submit \
	--master spark://${SPARK_MASTER_IP}:${SPARK_MASTER_PORT}  \
	--conf spark.driver.host=${SPARK_LOCAL_IP} \
	--properties-file /spark-defaults.conf \
	--jars /home/jars/spark-cassandra-connector_2.10-1.0.0-rc4.jar,/home/jars/spark-cassandra-connector-java_2.10-1.0.0-rc4.jar \
	--conf spark.cassandra.connection.host=${CASSANDRA_IP} \
	--class ca.yorku.ceras.cvstsparkjobengine.job.SipreskJob \
	/home/CVSTSparkJobEngine-0.0.1-SNAPSHOT.jar
	"$@"