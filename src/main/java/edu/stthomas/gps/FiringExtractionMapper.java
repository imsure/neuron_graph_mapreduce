package edu.stthomas.gps;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FiringExtractionMapper
extends Mapper<IntWritable, MultiWritableWrapper, IntWritable, IntWritable>{

        private IntWritable firing_time = new IntWritable();
        
        @Override
        public void map(IntWritable key, MultiWritableWrapper value, Context context)
                        throws IOException, InterruptedException {
                
                NeuronWritable neuron = value.getNeuronWritable();
                if (neuron.fired == 'Y') {
                        firing_time.set(neuron.time);
                        context.write(key, firing_time);
                }
        }
}