package edu.stthomas.gps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class NeuronStateWritable implements Writable {

	// Used to identify if this writable is a neuron structure 
	// or just containing a synaptic weight value.
	private char typeOfValue = 0; // 'N': neuron structure; 'W': synaptic weight
	private float weight = 0;
	private NeuronWritable neuron = null;
	
	public NeuronStateWritable() {
		neuron = new NeuronWritable();
	}
	
	public void setTypeOfValue(char type) {
		this.typeOfValue = type;
	}
	
	public void setWeight(float weight) {
		this.weight = weight;
	}
	
	public char getTypeOfValue() {
		return this.typeOfValue;
	}
	
	public float getWeight() {
		return this.weight;
	}
	
	public void setNeuron(NeuronWritable neuron) {
		this.neuron = neuron;
	}
	
	public NeuronWritable getNeuron() {
		return this.neuron;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeChar(typeOfValue);
		out.writeFloat(weight);
		if (typeOfValue == 'N') {
			neuron.write(out);
		}
	}
	
	public void readFields(DataInput in) throws IOException {
		typeOfValue = in.readChar();
		weight = in.readFloat();
		if (typeOfValue == 'N') {
			neuron.readFields(in);
		}
	}
}
