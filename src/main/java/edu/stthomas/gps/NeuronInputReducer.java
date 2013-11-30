package edu.stthomas.gps;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class NeuronInputReducer 
extends Reducer<IntWritable, NeuronWritable, IntWritable, NeuronWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<NeuronWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		/*
		 * Simply emit key, value pairs.
		 */
		for (NeuronWritable value : values) {
			context.write(key, value);
		}
	}
}
