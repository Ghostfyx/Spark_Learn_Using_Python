#!/usr/bin/env sh
cd /Users/yuexiangfan/coding/PythonProject/Spark_Learn_Using_Python/Learning_Spark_Fast_Data_Analysis/chap10/
rm -rf logs
touch logs
tail -f logs | nc -l 7777 &
TAIL_NC_PID=$!
cat ./fake_logs/log1.log >> logs
sleep 5
cat ./fake_logs/log2.log >> logs
sleep 1
cat ./fake_logs/log1.log >> logs
sleep 2
cat ./fake_logs/log1.log >> logs
sleep 3
sleep 20
kill $TAIL_NC_PID
