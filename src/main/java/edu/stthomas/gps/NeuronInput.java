package edu.stthomas.gps;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class NeuronInput extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input> <neuron output> <adjacency list output>\n", 
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		String input = args[0];
		String neuron_output = args[1];
		String adjlist_output = args[2];
		
		Job job = new Job(getConf());
		
		job.setJarByClass(this.getClass());
		job.setJobName("Neuron Graph Generation");
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(neuron_output));
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		
		job.setMapperClass(NeuronInputMapper.class);
		job.setReducerClass(NeuronInputReducer.class);
		job.setPartitionerClass(InputPartitioner.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		// Range paritioner decides how many reducers we need.
		job.setNumReduceTasks(InputPartitioner.TotalNumOfNeurons 
				/ InputPartitioner.NumOfNeuronsPerPartition);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NeuronWritable.class);
		
		boolean success = job.waitForCompletion(true);
		if (success == true) {
			Job job2 = new Job(getConf());
			
			job2.setJarByClass(this.getClass());
			job2.setJobName("Adjacency List Generation");
			
			FileInputFormat.addInputPath(job2, new Path(input));
			FileOutputFormat.setOutputPath(job2, new Path(adjlist_output));
			
			FileOutputFormat.setCompressOutput(job2, true);
			FileOutputFormat.setOutputCompressorClass(job2, SnappyCodec.class);
			
			SequenceFileOutputFormat.setOutputCompressionType(job2, CompressionType.BLOCK);
			
			job2.setMapperClass(AdjListMapper.class);
			job2.setReducerClass(AdjListReducer.class);
			job2.setPartitionerClass(AdjListPartitioner.class);
			
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			// Range paritioner decides how many reducers we need.
			job2.setNumReduceTasks(AdjListPartitioner.TotalNumOfNeurons 
					/ AdjListPartitioner.NumOfNeuronsPerPartition);
			
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(AdjListWritable.class);
			
			success = job2.waitForCompletion(true);
		}		
		
		return success ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new NeuronInput(), args);
		System.exit(res);
	}
}
