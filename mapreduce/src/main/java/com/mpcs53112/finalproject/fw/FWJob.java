package com.mpcs53112.finalproject.fw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FWJob extends BaseJob {
	public static final String JOBNAME_PREFIX = "Floyd-Warshall"; 
	
	static enum Iterations {
		numberOfIterations
	}
	
	static enum Numbers {
		numberOfNodes
	}
	
	private Job getJob(String jobName, String[] args) throws Exception {
		JobInfo jobInfo = new JobInfo() {
			@Override
			public Class<? extends Reducer<Text, Text, Text, Text>> getCombinerClass() {
				return null;
			}
			
			@Override
			public Class<?> getJarByClass() {
				return FWJob.class;
			}
			
			@Override
			public Class<? extends Mapper<Object, Text, Text, Text>> getMapperClass() {
				return FWMapper.class;
			}
			
			@Override
			public Class<?> getOutputKeyClass() {
				return Text.class;
			}
			
			@Override
			public Class<?> getOutputValueClass() {
				return Text.class;
			}
			
			@Override
			public Class<? extends Reducer<Text, Text, Text, Text>> getReducerClass() {
				return FWReducer.class;
			}
		};
		return super.setupJob(jobName, jobInfo);
	}

	@Override
	public int run(String[] args) throws Exception {
		int iterationCount = 1;
		int numberOfNodes = Integer.parseInt(args[0]);
		Job job;
		
		while (iterationCount <= numberOfNodes) {
			job = getJob(JOBNAME_PREFIX + "_" + numberOfNodes, args);
			
			String input_path, output_path;
			
			if (iterationCount == 1)
				input_path = args[1];
			else
				input_path = args[1] + iterationCount;
			
			output_path = args[2] + (iterationCount + 1);
			
			FileInputFormat.setInputPaths(job, new Path(input_path));
			FileOutputFormat.setOutputPath(job, new Path(output_path));
			
			job.waitForCompletion(true);
			
			job.getCounters().findCounter(Iterations.numberOfIterations).setValue(iterationCount);
			job.getCounters().findCounter(Numbers.numberOfNodes).setValue(numberOfNodes);
			iterationCount++;
		}
		
		return 0;
	}

	public static void main(String args[]) throws Exception {
		int result = ToolRunner.run(new Configuration(), new FWJob(), args);
		if (args.length != 3) {
			System.err.println("Usage: <number of nodes> <input directory path> <output directory path>");
			System.exit(1);
		}
		System.exit(result);
	}
}