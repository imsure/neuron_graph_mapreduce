package edu.stthomas.gps;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The Mapper is intended to re-organize the outputs of the previous MR
 * jobs for neuron processing. The intention is to produce a hierarchy of
 * directory to partition the outputs by neuron id and simulation time for hive to process.
 * For example, /path/to/basedir/id=1/time=10/data-associated-with-the-neuron-1-at-10ms
 * 
 * Input key: neuron id
 * Input value: neuron's internal state
 * 
 * Output key: null (because we will store the neuron id as the name of directory.
 * Output value: text with data associated with a neuron at a given time
 * 
 * @author imsure
 *
 */
public class Output4HiveMapper 
extends Mapper<IntWritable, NeuronWritable, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	private NeuronWritable neuron = new NeuronWritable();
	private Text data = new Text();
		
	@Override
	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}
	
	@Override
	public void map(IntWritable key, NeuronWritable value, Context context) 
			throws IOException, InterruptedException {
		int neuron_id = key.get();
		int time = value.time;
		//String basepath = String.format("id=%s/time=%/part", neuron_id, time);
		String path = new String();
		path = "id=" + neuron_id + "/time=" + time + "/neuron";
		data.set(value.toString2());
		multipleOutputs.write(NullWritable.get(), data, path);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}


