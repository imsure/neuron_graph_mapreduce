package edu.stthomas.gps;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;

/*
 * Generates read-only adjacency list.
 */
public class AdjListMapper extends Mapper<LongWritable, Text, IntWritable, AdjListWritable> {

	private Random randn = new Random();
	private IntWritable neuron_id = new IntWritable();

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
		
		for (int i = start_id; i <= end_id; i++) {
			neuron_id.set(i);
			
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
						weight.setWeight((float)0.2*randn.nextFloat());
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
						
			context.write(neuron_id, AdjListWritable.fromArrayList(adjlist));
		}
	}
}
