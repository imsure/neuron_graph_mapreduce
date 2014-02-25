package edu.stthomas.gps;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The reducer is used to group the information of all neurons at a certain time
 * and write to a single file in HDFS. The file is like a snapshot of the neurons
 * which can be used by hive to do post-analysis.
 * 
 * Note that we must have a partitioner that guarantees that all neurons associated
 * with a certain time must go to the same reducer.
 * 
 * Input key: neuron id
 * Input value: NeuronHiveWritable
 * 
 * Output key: null
 * Output value: Text format of NeuronHiveWritable
 * 
 * @author imsure
 *
 */
public class NeuronHiveReducer 
extends Reducer<IntWritable, NeuronHiveWritable, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	private Text data = new Text();
	
	@Override
	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<NeuronHiveWritable> values, Context context) 
			throws IOException, InterruptedException {
		String path = "" + "time=" + key;
		for (NeuronHiveWritable neuron : values) {
			data.set(neuron.toString());
			multipleOutputs.write(NullWritable.get(), data, path);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
