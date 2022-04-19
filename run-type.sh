#!/bin/bash
hadoop fs -rm -r /user/my2400/project/output/complaints_type
time hadoop jar gzac.jar GroupComplaintsType /user/my2400/project/input/crime-data-clean.csv /user/my2400/project/output/complaints_type
hadoop fs -ls project/output/complaints_type