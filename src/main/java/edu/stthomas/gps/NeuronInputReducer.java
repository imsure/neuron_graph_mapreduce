package edu.stthomas.gps;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class NeuronInputReducer 
extends Reducer<IntWritable, MultiWritableWrapper, IntWritable, MultiWritableWrapper> {

	@Override
	public void reduce(IntWritable key, Iterable<MultiWritableWrapper> values, Context context) 
			throws IOException, InterruptedException {
		
		/*
		 * Simply emit key, value pairs.
		 */
		for (MultiWritableWrapper value : values) {
			context.write(key, value);
		}
	}
}
