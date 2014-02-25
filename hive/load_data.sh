#!/bin/sh

time_max=20
time=1

while [ $time -le $time_max ]
do
    hive -e "ALTER TABLE neurons ADD PARTITION (time=${time});"
    time=$(($time+1))
done
