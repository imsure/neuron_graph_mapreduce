package edu.stthomas.gps;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class NeuronGraphReducer extends Reducer<IntWritable, MultiWritableWrapper, IntWritable, MultiWritableWrapper> {

	private MultiWritableWrapper multi_writable = new MultiWritableWrapper(); // value
	private NeuronWritable neuron;
	private AdjListWritable adjlist;
	
	private enum Test {
		test, weights_count, neuron_count,
	}
	
	private boolean isNeuron(MultiWritableWrapper value) {
		if (value.getWritableType() == MultiWritableWrapper.Synaptic_Weight) {
			return false;
		} else {
			return true;
		}
	}
	
	public void reduce(IntWritable key, Iterable<MultiWritableWrapper> values, Context context) 
			throws IOException, InterruptedException {
		
		float weight_sum = 0;
		
		for (MultiWritableWrapper value : values) {
			if (!isNeuron(value)) {
				//context.getCounter(Test.weights_count).increment(1);
				weight_sum += value.getWeight();
			} else { // Resume the neuron structure.
				neuron = value.getNeuronWritable();
				adjlist = value.getAdjListWritable();
				//context.getCounter(Test.neuron_count).increment(1);
			}
		}
		
		// Update synaptic summation for the next iteration. This is the 
		// only information needs to be updated.
		neuron.synaptic_sum = weight_sum;
		multi_writable.setWritableType(MultiWritableWrapper.NeuronObj);
		multi_writable.setWeight(999);
		multi_writable.setNeuronWritable(neuron);
		multi_writable.setAdjListWritable(adjlist);
		
		if (multi_writable.getWritableType() == MultiWritableWrapper.Synaptic_Weight) {
			context.getCounter(Test.test).increment(1);
		}
		
		context.write(key, multi_writable);
	}
}
