package edu.stthomas.gps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*
 * A wrapper class for two different writables NeuronWritable and AdjListWritable.
 * NeuronWritable represents meta data for a neuron and its internal state.
 */
public class MultiWritableWrapper implements Writable {

	private NeuronWritable neuron = null;
	private AdjListWritable adjlist = null;
	
	public MultiWritableWrapper () {
		neuron = new NeuronWritable();
		adjlist = new AdjListWritable();
	}
	
	public void write(DataOutput out) throws IOException {
		neuron.write(out);
		adjlist.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		neuron.readFields(in);
		adjlist.readFields(in);
	}
	
	public void setNeuronWritable(NeuronWritable neuron) {
		this.neuron = neuron;
	}
	
	public void setAdjListWritable(AdjListWritable adjlist) {
		this.adjlist = adjlist;
	}
	
	public NeuronWritable getNeuronWritable() {
		return this.neuron;
	}
	
	public AdjListWritable getAdjListWritable() {
		return this.adjlist;
	}
	
	@Override
	public String toString() {
		return neuron.toString() + "\tSize of Adjacency list: " + adjlist.printSize();
	}
}
