package edu.stthomas.gps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NeuronGraphMapper extends Mapper<IntWritable, MultiWritableWrapper, IntWritable, MultiWritableWrapper> {

	private IntWritable neuron_id = new IntWritable(); // key
	private MultiWritableWrapper multi_writable = new MultiWritableWrapper(); // value
	private Random randn = new Random();
	private Map<Integer, Float> weight_map; // For in-mapper combiner


	private enum Firing {
		Count,
	}

	@Override
	public void setup(Context context)
			throws IOException, InterruptedException {
		// Set up Hash Map for in mapper combiner.
		this.weight_map = new HashMap<Integer, Float>();
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

		// Check if the neuron has fired.
		if (neuron.potential >= 30.0) { // fired
			AdjListWritable adjlist_writable = value.getAdjListWritable();
			System.err.println(value.getWritableType() + "\t" + value.getWeight() + value.getNeuronWritable().toString());
			ArrayList<SynapticWeightWritable> adjlist = adjlist_writable.toArrayList();

			// Emit synaptic weights by iterating the adjacency list.
			for (SynapticWeightWritable weight : adjlist) {

				int id = weight.getID();

				Float accumulated_weight = this.weight_map.get(id);
				float current_weight = weight.getWeight();
				if (accumulated_weight == null) {
					this.weight_map.put(id, current_weight);
				} else {
					this.weight_map.put(id, accumulated_weight+current_weight);
				}
			}

			// Reset the membrane potential (voltage) and membrane recovery variable after firing.
			neuron.potential = neuron.param_c;
			neuron.recovery += neuron.param_d;
			neuron.fired = 'Y'; // Indicate the neuron fired at this iteration.

			context.getCounter(Firing.Count).increment(1);
		}

		// Update the multiple writable with the updated neuron object after evolution. 
		// Other fileds in the multiple writable remained the same.
		value.setNeuronWritable(neuron);

		//value.setWritableType(MultiWritableWrapper.NeuronObj);

		// Emit the whole neuron structure.
		context.write(key, value);
	}

	@Override
	public void cleanup(Context context)
			throws IOException, InterruptedException {

		for (Map.Entry<Integer, Float> entry : this.weight_map.entrySet()) {
			multi_writable.setWritableType(MultiWritableWrapper.Synaptic_Weight);
			multi_writable.setWeight(entry.getValue());
			multi_writable.setNeuronWritable(null);
			multi_writable.setAdjListWritable(null);

			neuron_id.set(entry.getKey());

			context.write(neuron_id, multi_writable);
		}
	}
}
