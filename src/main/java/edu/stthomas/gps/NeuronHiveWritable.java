package edu.stthomas.gps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The writable is intended to transform the 'NeuronWritable' object
 * into a slightly different format in order to export data to hive.
 * 
 * I want to partition the hive table by id, so I need a writable which
 * can hold other information of a neuron, like id, type, etc.
 * 
 * @author imsure
 *
 */
public class NeuronHiveWritable {

	public char type = 0;
	public int id = 0;
	public float recovery = 0;
	public float potential = 0;
	public float synaptic_sum = 0;
	public char fired = 0;
	
	public NeuronHiveWritable() {}
	
	public void write(DataOutput out) throws IOException {
		out.writeChar(type);
		out.writeInt(id);
		out.writeFloat(recovery);
		out.writeFloat(potential);
		out.writeFloat(synaptic_sum);
		out.writeChar(fired);
	}
	
	public void readFields(DataInput in) throws IOException {
		type = in.readChar();
		id = in.readInt();
		recovery = in.readFloat();
		potential = in.readFloat();
		synaptic_sum = in.readFloat();
		fired = in.readChar();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(type).append(',');
		sb.append(id).append(',');
		sb.append(recovery).append(',');
		sb.append(potential).append(',');
		sb.append(synaptic_sum).append(',');
		sb.append(fired);
		
		return sb.toString();
	}
}
