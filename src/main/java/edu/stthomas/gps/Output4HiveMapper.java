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
 * Output key: neuron id
 * Output value: NeuronHiveWritable
 * 
 * @author imsure
 *
 */
public class Output4HiveMapper 
extends Mapper<IntWritable, NeuronWritable, IntWritable, NeuronHiveWritable> {

	private NeuronHiveWritable neuron = new NeuronHiveWritable();
	private IntWritable time = new IntWritable();
	
	@Override
	public void map(IntWritable key, NeuronWritable value, Context context) 
			throws IOException, InterruptedException {
	
		time.set(value.time); // set time as key

		neuron.id = key.get();
		neuron.type = value.type;
		neuron.potential = value.potential;
		neuron.recovery = value.recovery;
		neuron.fired = value.fired;
		neuron.synaptic_sum = value.synaptic_sum;
		
		context.write(time, neuron);
	}
}
