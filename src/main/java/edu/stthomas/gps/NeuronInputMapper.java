package edu.stthomas.gps;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * The Mapper class will generate input data for a neuron network based on the metadata provided.
 *
 */
public class NeuronInputMapper extends Mapper<LongWritable, Text, IntWritable, MultiWritableWrapper>
{
	private IntWritable neuron_id = new IntWritable();
	private MultiWritableWrapper multi_writable = new MultiWritableWrapper();
	private Random randn = new Random();
	public static final float Excitatory_Prob = (float) 0.4;
	public static final float Inhibitory_Prob = (float) 0.6;

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

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
			ArrayList<SynapticWeightWritable> adjlist = new ArrayList<SynapticWeightWritable>();

			/*
			 * Go through outgoing nodes, create edges from neuron 'i' to neuron 'j', that is,
			 * synaptic weights that neuron 'i' have to neuron 'j'.
			 */
			if (type == 'e') {
				for (int j = 1; j <= total; j++) {
					if (randn.nextFloat() < Excitatory_Prob) {
						SynapticWeightWritable weight = new SynapticWeightWritable();
						weight.setID(j);
						weight.setWeight((float)0.3*randn.nextFloat());
						adjlist.add(weight);
					}
				}
			} else {
				for (int j = 1; j <= total; j++) {
					if (randn.nextFloat() < Inhibitory_Prob) {
						SynapticWeightWritable weight = new SynapticWeightWritable();
						weight.setID(j);
						weight.setWeight((float)-0.5*randn.nextFloat());
						adjlist.add(weight);
					}
				}
			}
			
			multi_writable.setWritableType(MultiWritableWrapper.NeuronObj);
			multi_writable.setWeight(999); // just an arbitrary number, as a place holder
			multi_writable.setNeuronWritable(neuron);
			multi_writable.setAdjListWritable(AdjListWritable.fromArrayList(adjlist));
			
			// The following commented code is just to test if writing null to writable is ok. It is fine actually.
			//multi_writable.setWritableType(MultiWritableWrapper.Synaptic_Weight);
			//multi_writable.setNeuronWritable(null);
			//multi_writable.setAdjListWritable(null);
			
			context.write(neuron_id, multi_writable);
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
