package edu.stthomas.gps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MultiWritableWrapper implements Writable {

	public static final int NeuronObj = 0;
	public static final int Synaptic_Weight = 1;
	
	private int writable_type;
	private float weight;
	private NeuronWritable neuron;
	private AdjListWritable adjlist;
	
	public MultiWritableWrapper () {
		neuron = new NeuronWritable();
		adjlist = new AdjListWritable();
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(writable_type);
		out.writeFloat(weight);
		
		if (writable_type == NeuronObj) {
			neuron.write(out);
			adjlist.write(out);
		}
	}
	
	public void readFields(DataInput in) throws IOException {
		writable_type = in.readInt();
		weight = in.readFloat();
		
		if (writable_type == NeuronObj) {
			neuron.readFields(in);
			adjlist.readFields(in);
		}
	}
	
	public void setWritableType(int type) {
		this.writable_type = type;
	}
	
	public void setWeight(float weight) {
		this.weight = weight;
	}
	
	public void setNeuronWritable(NeuronWritable neuron) {
		this.neuron = neuron;
	}
	
	public void setAdjListWritable(AdjListWritable adjlist) {
		this.adjlist = adjlist;
	}
	
	public int getWritableType() {
		return this.writable_type;
	}
	
	public float getWeight() {
		return this.weight;
	}
	
	public NeuronWritable getNeuronWritable() {
		return this.neuron;
	}
	
	public AdjListWritable getAdjListWritable() {
		return this.adjlist;
	}
	
	@Override
	public String toString() {
		if (this.writable_type == Synaptic_Weight) {
			return Float.toString(weight);
		} else {
			return neuron.toString();
			//return neuron.toString() + "\t" + adjlist.toString();
		}
	}
}
