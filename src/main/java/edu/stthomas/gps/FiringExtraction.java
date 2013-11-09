package edu.stthomas.gps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FiringExtraction extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.printf("Usage: %s [generic options] <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}

		/*
		 * Start jobs for post-analysis
		 */
		String inpaths = new String(); // used to construct a comma separated input paths.

		for (int i = 1; i < NeuronGraph.TIME_IN_MS; i++) {
			inpaths += "neuron_graph_output" + i + ",";
		}
		inpaths += "neuron_graph_output" + NeuronGraph.TIME_IN_MS;

		Job job = new Job(getConf());
		job.setJarByClass(this.getClass());
		job.setJobName("Fired Neurons Extraction");

		FileInputFormat.addInputPaths(job, inpaths);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		job.setMapperClass(FiringExtractionMapper.class);
		job.setReducerClass(FiringExtractionReducer.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new FiringExtraction        (), args);
		System.exit(res);
	}
}