#!/bin/sh

neuron_id_max=1000
time_max=20

neuron_id=1
time=1

while [ $neuron_id -le $neuron_id_max ]
do
    hive -e "ALTER TABLE neurons ADD PARTITION (id=$(neuron_id), time=$(time));"
    neuron_id=$(($neuron_id+1))
done
