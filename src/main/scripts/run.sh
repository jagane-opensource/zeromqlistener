#!/bin/bash
CP=`mvn dependency:build-classpath|grep -v INFO | grep -v WARN`

JAR=target/infinstor-zeromqlistener-1.0-SNAPSHOT.jar

java -cp "$CP":$JAR com.infinstor.bitcoin.zeromqlistener.Listener $*
