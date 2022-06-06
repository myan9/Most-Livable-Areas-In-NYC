#!/bin/bash
rm *.class
rm *.jar

hadoop fs -rm -r /user/ks6401/proj/zip2Name

javac -classpath `hadoop classpath` zip2NameMapper.java
javac -classpath `hadoop classpath` zip2NameReducer.java
javac -classpath `hadoop classpath`:. zip2Name.java

jar cvf zip2Name.jar *.class

hadoop jar zip2Name.jar zip2Name /user/ks6401/proj/zipComplaintCount /user/ks6401/proj/zip2Name

