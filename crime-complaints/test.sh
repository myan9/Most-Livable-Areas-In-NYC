#!/bin/bash
hadoop fs -ls project/output;
hadoop fs -cat project/output/part-r-00000;
hadoop fs -tail project/output/part-r-00000;
hadoop fs -mkdir project/input
hadoop fs -mv project/output/part-r-00000 project/input/crime-data-clean.csv;
hadoop fs -mv project/- project/crime-data.json;
# 10001 - 11697

