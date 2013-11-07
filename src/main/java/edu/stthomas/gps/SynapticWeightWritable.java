package edu.stthomas.gps;

import org.apache.hadoop.io.Writable;

import java.io.*;

public class SynapticWeightWritable implements Writable {
	
	private int id;
	private float weight;
	
	public SynapticWeightWritable() {}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeFloat(weight);
	}
	
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		weight = in.readFloat();
	}
	
	public void setID(int id) {
		this.id = id;
	}
	
	public void setWeight(float weight) {
		this.weight = weight;
	}
	
	public int getID() {
		return id;
	}
	
	public float getWeight() {
		return weight;
	}
	
	@Override
	public String toString() {
		return this.id + ":" + this.weight;
	}
}
