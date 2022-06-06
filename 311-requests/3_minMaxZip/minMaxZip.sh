#!/bin/bash
hadoop fs -rm -r /user/ks6401/mini_proj/minMaxZip

javac -classpath `hadoop classpath` minMaxZipMapper.java
javac -classpath `hadoop classpath` minMaxZipReducer.java
javac -classpath `hadoop classpath`:. minMaxZip.java

jar cvf minMaxZip.jar *.class

hadoop jar minMaxZip.jar minMaxZip /user/ks6401/mini_proj/311_filter/part-r-00000 /user/ks6401/mini_proj/minMaxZip

