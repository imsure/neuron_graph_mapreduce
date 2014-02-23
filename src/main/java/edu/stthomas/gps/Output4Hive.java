package edu.stthomas.gps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The driver for re-organizing outputs from each iteration of
 * neuron processing into a hierarchy of directories with a structure
 * like: /path/to/basedir/id=4/time=10/file in order to facilitate 
 * hive queries.
 * 
 * @author imsure
 *
 */
public class Output4Hive extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", 
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}

		/*
		 * Start jobs for post-analysis
		 */
		String inpaths = new String(); // used to construct a comma separated input paths.

		Job job = new Job(getConf());
		job.setJarByClass(this.getClass());
		job.setJobName("Output 4 Hive");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(Output4HiveMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new Output4Hive(), args);
		System.exit(res);
	}
}
