#!/bin/bash
rm *.class
rm *.jar

hadoop fs -rm -r /user/ks6401/proj/0_examineToken

javac -classpath `hadoop classpath` examineTokenMapper.java
javac -classpath `hadoop classpath` examineTokenReducer.java
javac -classpath `hadoop classpath`:. examineToken.java

jar cvf examineToken.jar *.class

hadoop jar examineToken.jar examineToken /user/ks6401/proj/311_data.csv /user/ks6401/proj/0_examineToken
