#!/bin/bash
spark-submit --class "TwitterHashTagCount" --master local[4] target/streaming-project-1.0.jar $1 $2 2>err_log.txt
