#!/bin/bash
hadoop fs -rm -r /user/my2400/project/output/clean
time hadoop jar gzac.jar GetZipcodeAndClean /user/my2400/project/crime-data.csv /user/my2400/project/output/clean
hadoop fs -tail project/output/project/output/clean/part-r-00000;
# hadoop jar cap.jar CleanAndProfile /user/my2400/project/crime-data.csv /user/my2400/project/output
# hadoop jar cap.jar CleanAndProfile /user/my2400/project/small-crime.csv /user/my2400/project/output
# hadoop fs -rm -r /user/my2400/project/output/total_complaints
# hadoop jar gzac.jar GetTotalComplaints /user/my2400/project/input/crime-data-clean.csv /user/my2400/project/output/total_complaints
# hadoop fs -rm -r /user/my2400/project/output/total_complaints
# hadoop jar gtc.jar GetTotalComplaints /user/my2400/project/crime-data-clean.csv /user/my2400/project/output/total_complaints
