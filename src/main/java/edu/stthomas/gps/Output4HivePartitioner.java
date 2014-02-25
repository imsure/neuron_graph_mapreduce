package edu.stthomas.gps;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The partitioner is working by partitioning based on the simulation time.
 * All neurons at a certain time go to the same reducer.
 * 
 * @author imsure
 *
 */
public class Output4HivePartitioner extends Partitioner<IntWritable, NeuronHiveWritable> {

	@Override
	public int getPartition(IntWritable key, NeuronHiveWritable value, int numReduceTasks) {
		return (key.get() - 1) % numReduceTasks;
	}
}
