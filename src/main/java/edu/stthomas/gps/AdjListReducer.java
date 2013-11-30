package edu.stthomas.gps;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class AdjListReducer 
extends Reducer<IntWritable, AdjListWritable, IntWritable, AdjListWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<AdjListWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		/*
		 * Simply emit key, value pairs.
		 */
		for (AdjListWritable value : values) {
			context.write(key, value);
		}
	}
}
