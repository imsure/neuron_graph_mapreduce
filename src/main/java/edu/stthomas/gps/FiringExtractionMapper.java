package edu.stthomas.gps;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FiringExtractionMapper 
extends Mapper<IntWritable, NeuronWritable, IntWritable, IntWritable>{

	private IntWritable firing_time = new IntWritable();
	
	@Override
	public void map(IntWritable key, NeuronWritable value, Context context) 
			throws IOException, InterruptedException {
		
		if (value.fired == 'Y') {
			firing_time.set(value.time);
			context.write(key, firing_time);
		}
	}
}
