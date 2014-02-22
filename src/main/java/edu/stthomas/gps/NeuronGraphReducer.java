package edu.stthomas.gps;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;

/**
 * Input key: neuron id
 * Input value: neuron's internal state
 * 
 * Output key: neuron id
 * Output value: neuron's internal state with synaptic weight updated
 * 
 * @author imsure
 *
 */
public class NeuronGraphReducer extends Reducer<IntWritable, NeuronStateWritable, IntWritable, NeuronWritable> {
	
	private NeuronWritable neuron = new NeuronWritable();
	
	private enum Test {
		neg_weights_count, neuron_count, test,
	}
	
	private boolean isNeuron(NeuronStateWritable value) {
		if (value.getTypeOfValue() == 'W') {
			return false;
		} else {
			return true;
		}
	}
	
	public void reduce(IntWritable key, Iterable<NeuronStateWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		float weight_sum = 0;
		
		for (NeuronStateWritable value : values) {
			if (!isNeuron(value)) {
				float weight = value.getWeight();
				weight_sum += weight;
			} else { // Recover the neuron structure.
				neuron = value.getNeuron();
			}
		}
		
		// Update synaptic summation for the next iteration. This is the 
		// only information needs to be updated.
		neuron.synaptic_sum = weight_sum;
		
		context.write(key, neuron);
	}
}
