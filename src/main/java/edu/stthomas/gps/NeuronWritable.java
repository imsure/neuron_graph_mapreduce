package edu.stthomas.gps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;

public class NeuronWritable implements Writable {

	public char type = 0;
	public int time = 0; // In simulation, it is actually the number iteration
	public float param_a = 0, param_b = 0, param_c = 0, param_d = 0;
	public float recovery = 0;
	public float potential = 0;
	public float synaptic_sum = 0;
	public char fired = 0;
	
	// Used to identify if this writable is a neuron structure 
	// or just containing a synaptic weight value.
	private char typeOfValue = 0;
	private float weight = 0;
	
	public NeuronWritable() {
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
	
	public void write(DataOutput out) throws IOException {
		out.writeChar(type);
		out.writeInt(time);
		out.writeFloat(param_a);
		out.writeFloat(param_b);
		out.writeFloat(param_c);
		out.writeFloat(param_d);
		out.writeFloat(recovery);
		out.writeFloat(potential);
		out.writeFloat(synaptic_sum);
		out.writeChar(fired);
		
		out.writeChar(typeOfValue);
		out.writeFloat(weight);
	}
	
	public void readFields(DataInput in) throws IOException {
		type = in.readChar();
		time = in.readInt();
		param_a = in.readFloat();
		param_b = in.readFloat();
		param_c = in.readFloat();
		param_d = in.readFloat();
		recovery = in.readFloat();
		potential = in.readFloat();
		synaptic_sum = in.readFloat();
		fired = in.readChar();
		
		typeOfValue = in.readChar();
		weight = in.readFloat();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(type).append(',');
		sb.append(time).append(',');
		sb.append(param_a).append(',');
		sb.append(param_b).append(',');
		sb.append(param_c).append(',');
		sb.append(param_d).append(',');
		sb.append(recovery).append(',');
		sb.append(potential).append(',');
		sb.append(synaptic_sum).append(',');
		sb.append(fired).append(',');
		sb.append(typeOfValue).append(',');
		sb.append(weight);
		
		return sb.toString();
	}
}
