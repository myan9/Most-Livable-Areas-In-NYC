#!/bin/bash
rm *.class
rm *.jar

hadoop fs -rm -r /user/ks6401/proj/1_filter
hadoop fs -rm /user/ks6401/proj/311_filtered

javac -classpath `hadoop classpath` dataFilterMapper.java
javac -classpath `hadoop classpath` dataFilterReducer.java
javac -classpath `hadoop classpath`:. dataFilter.java

jar cvf dataFilter.jar *.class
hadoop jar dataFilter.jar dataFilter /user/ks6401/proj/311_data.csv /user/ks6401/proj/1_filter

hadoop fs -mv /user/ks6401/proj/1_filter/part-r-00000 /user/ks6401/proj/311_filtered
