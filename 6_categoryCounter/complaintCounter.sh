#!/bin/bash
hadoop fs -rm -r /user/ks6401/proj/2_complaintCounter

javac -classpath `hadoop classpath` complaintCounterMapper.java
javac -classpath `hadoop classpath` complaintCounterReducer.java
javac -classpath `hadoop classpath`:. compalintCounter.java

jar cvf complaintCounter.jar *.class

hadoop jar complaintCounter.jar complaintCounter /user/ks6401/proj/311_filtered /user/ks6401/proj/2_complaintCounter

