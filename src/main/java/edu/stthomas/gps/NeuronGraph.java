package edu.stthomas.gps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NeuronGraph extends Configured implements Tool {

	public final static int TIME_IN_MS = 100;
	private int time_of_simulation = 0;
	
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}

		int timer = 1;
		boolean success = false;

		String IN = args[0];
		String OUT = args[1] + timer;

		// Chaining the jobs together
		while (timer <= TIME_IN_MS) {

			Job job = new Job(getConf());

			job.setJarByClass(this.getClass());
			job.setJobName("Neuron Graph Processing:" + timer);

			FileInputFormat.addInputPath(job, new Path(IN));
			FileOutputFormat.setOutputPath(job, new Path(OUT));

			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

			SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

			job.setMapperClass(NeuronGraphMapper.class);
			job.setReducerClass(NeuronGraphReducer.class);
			job.setPartitionerClass(NeuronIDRangePartitioner.class);
			
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			//job.setNumReduceTasks(0);
			job.setNumReduceTasks(NeuronIDRangePartitioner.TotalNumOfNeurons /
					NeuronIDRangePartitioner.NumOfNeuronsPerPartition);

			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(NeuronStateWritable.class);
			
			job.setOutputKeyClass(IntWritable.class);
			//job.setOutputValueClass(NeuronWritable.class);
			job.setOutputValueClass(MultiWritableWrapper.class);

			success = job.waitForCompletion(true);
			if (success == false) {
				break;
			}

			// Reset the input and output path for the next iteration.
			// Current output becomes the input for the next iteration.
			IN = OUT;
			timer++;
			OUT = args[1] + timer;
		}

		return (success ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new NeuronGraph(), args);
		System.exit(res);
	}
}
