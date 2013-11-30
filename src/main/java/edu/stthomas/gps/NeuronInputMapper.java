package edu.stthomas.gps;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;


/**
 * The Mapper class will generate input data for a neuron network based on the metadata provided.
 *
 */
public class NeuronInputMapper extends Mapper<LongWritable, Text, IntWritable, NeuronWritable>
{
	private IntWritable neuron_id = new IntWritable();
	private Random randn = new Random();

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		//Configuration conf = context.getConfiguration();
		//float ex_prob = conf.getFloat("EXPROB", Excitatory_Prob);
		//float in_prob = conf.getFloat("INPROB", Inhibitory_Prob);
		
		String[] fields = value.toString().split(",");
		int start_id = Integer.parseInt(fields[0]);
		int end_id = Integer.parseInt(fields[1]);
		int total = Integer.parseInt(fields[2]);
		char type = fields[3].charAt(0);

		/*
		 * Iterate through start neuron id to end neuron id.
		 */
		for (int i = start_id; i <= end_id; i++) {
			neuron_id.set(i);
			NeuronWritable neuron = generateKey(type);
			context.write(neuron_id, neuron);
		}
	}

	private NeuronWritable generateKey(char type) {
		NeuronWritable neuron = new NeuronWritable();
		float randf = randn.nextFloat();

		/*
		 * 4 parameters differ as the type of neuron changes
		 */
		if (type == 'e') {
			neuron.param_a = (float) 0.02;
			neuron.param_b = (float) 0.2;
			neuron.param_c = -65 + 15 * randf * randf;
			neuron.param_d = 8 - 6 * randf * randf;
		} else {
			neuron.param_a = (float) (0.02 + 0.08 * randf);
			neuron.param_b = (float) (0.25 - 0.05 * randf);
			neuron.param_c = -65;
			neuron.param_d = 2;
		}

		neuron.potential = -65;
		neuron.recovery = neuron.potential * neuron.param_b;

		neuron.type = type;
		neuron.synaptic_sum = 0;
		neuron.fired = 'N';
		neuron.time = 0;
		
		return neuron;
	}
}
