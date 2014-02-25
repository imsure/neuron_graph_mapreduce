package edu.stthomas.gps;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.util.*;

public class AdjListWritable extends ArrayWritable {

	public AdjListWritable () {
		super(SynapticWeightWritable.class);
	}
	
	/*
	 * Instantiate an AdjListWritable from an ArrayList.
	 */
	public static AdjListWritable fromArrayList(ArrayList<SynapticWeightWritable> weights) {
		AdjListWritable adjlist = new AdjListWritable();
		SynapticWeightWritable[] weight_array = new SynapticWeightWritable[weights.size()];
		for (int i = 0; i < weights.size(); i++) {
			weight_array[i] = weights.get(i);
		}
		adjlist.set(weight_array);
		return adjlist;
	}
	
	/*
	 * Convert the array in the AdjListWritable to an ArrayList.
	 */
	public ArrayList<SynapticWeightWritable> toArrayList() {
		ArrayList<SynapticWeightWritable> adjlist = new ArrayList<SynapticWeightWritable>();
		for (Writable weight : this.get()) {
			adjlist.add((SynapticWeightWritable)weight);
		}
		return adjlist; 
	}
	
	/*
	 * Adjacency list could be very long. So use with caution.
	 */
	@Override
    public String toString() {
		ArrayList<SynapticWeightWritable> adjlist = this.toArrayList();
        
		StringBuilder sb = new StringBuilder();
        //String[] weights = super.toStrings();
        //for (String s : weights)
        //{
        //    sb.append(s).append(" ");
        //}
		
		int n = 0;
		for (SynapticWeightWritable weight : adjlist) {
			if (n < 10 || (adjlist.size()-n) < 10) { // just show the first 10 and last 10 elements
				sb.append(weight.toString()).append(' '); 
			}
			n++;
		}
        sb.append("size of ajacency list:").append(adjlist.size());
        return sb.toString();
    }
	
	public String printSize() {
		return Integer.toString(this.get().length);
	}
}
