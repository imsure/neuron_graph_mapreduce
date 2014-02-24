package edu.stthomas.gps;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;

/*
 * A simple range partitioner based on the neuron id which is an interger.
 */
public class AdjListPartitioner extends Partitioner<IntWritable, AdjListWritable> {
	
	@Override
	public int getPartition(IntWritable key, AdjListWritable value, int numReduceTasks) {
		/*
		 * The goal is to put neuron 1 to 200 to the reducer 0,
		 * neuron 201 to 400 to reducer 1, and so on.
		 */
		return (key.get() - 1) / NeuronInput.NumOfNeuronsPerPartition;
	}
}
