package edu.stthomas.gps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NeuronGraphMapper extends Mapper<IntWritable, MultiWritableWrapper, IntWritable, NeuronWritable> {

	private IntWritable neuron_id = new IntWritable(); // key
	private NeuronWritable synaptic_weight = new NeuronWritable();
	private Random randn = new Random();

	private enum Firing {
		Count,
	}
	
	private double getGaussian() {
		return randn.nextGaussian();
	}

	private void neuronEvolution(float current, NeuronWritable neuron) {
		current += neuron.synaptic_sum;
		// Update the membrane potential. Step 0.5 ms for numerical stability. 
		neuron.potential += 0.5 * (0.04*neuron.potential*neuron.potential + 5*neuron.potential
				+ 140 - neuron.recovery + current);
		neuron.potential += 0.5 * (0.04*neuron.potential*neuron.potential + 5*neuron.potential
				+ 140 - neuron.recovery + current);
		// Update membrane recovery variable.
		neuron.recovery += neuron.param_a * (neuron.param_b*neuron.potential - neuron.recovery);

		// Update number of iteration
		neuron.time += 1;
		neuron.synaptic_sum = (float) 0.0;
		neuron.fired = 'N'; // Reset firing status
	}

	@Override
	public void map(IntWritable key, MultiWritableWrapper value, Context context) 
			throws IOException, InterruptedException {

		float current;
		NeuronWritable neuron = value.getNeuronWritable();

		// Generate thalamic input.
		if (neuron.type == 'e') {
			current = 5 * (float)this.getGaussian();
		} else {
			current = 2 * (float)this.getGaussian();
		}

		// Start Neuron Evolution
		this.neuronEvolution(current, neuron);
		//System.err.println(neuron.toString());

		// Check if the neuron has fired.
		if (neuron.potential >= 30.0) { // fired
			AdjListWritable adjlist_writable = value.getAdjListWritable();
			//System.err.println(value.getWritableType() + "\t" + value.getWeight() + value.getNeuronWritable().toString());
			ArrayList<SynapticWeightWritable> adjlist = adjlist_writable.toArrayList();

			// Emit synaptic weights by iterating the adjacency list. 
			for (SynapticWeightWritable weight : adjlist) {
				synaptic_weight.setTypeOfValue('W');
				synaptic_weight.setWeight(weight.getWeight());

				neuron_id.set(weight.getID());

				context.write(neuron_id, synaptic_weight);
			}

			// Reset the membrane potential (voltage) and membrane recovery variable after firing.
			neuron.potential = neuron.param_c;
			neuron.recovery += neuron.param_d;
			neuron.fired = 'Y'; // Indicate the neuron fired at this iteration.
			
			context.getCounter(Firing.Count).increment(1);
		}
		
		// Emit the neuron structure.
		context.write(key, neuron);
	}
}
