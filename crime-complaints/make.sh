#!/bin/bash
rm -f gzac.jar;
javac -classpath `hadoop classpath` GetZipcodeAndCleanMapper.java;
javac -classpath `hadoop classpath` GetZipcodeAndCleanReducer.java;
javac -classpath `hadoop classpath` ZipcodeMapper.java;
javac -classpath `hadoop classpath` GetTotalComplaintsReducer.java;
javac -classpath `hadoop classpath`: GroupComplaintsTypeReducer.java;
javac -classpath `hadoop classpath`:. GetTotalComplaints.java;
javac -classpath `hadoop classpath`:. GetZipcodeAndClean.java;
javac -classpath `hadoop classpath`:. GroupComplaintsType.java;
jar cvf gzac.jar *.class;
