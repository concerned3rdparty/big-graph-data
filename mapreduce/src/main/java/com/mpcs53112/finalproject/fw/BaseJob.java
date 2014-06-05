package com.mpcs53112.finalproject.fw;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class BaseJob extends Configured implements Tool {
	
	protected abstract class JobInfo {
		public abstract Class<?> getJarByClass();
		public abstract Class<? extends Mapper<Object, Text, Text, Text>> getMapperClass();
		public abstract Class<? extends Reducer<Text, Text, Text, Text>> getCombinerClass();
		public abstract Class<? extends Reducer<Text, Text, Text, Text>> getReducerClass();
		public abstract Class<?> getOutputKeyClass();
		public abstract Class<?> getOutputValueClass();
		
 	}
	
	protected Job setupJob(String jobName, JobInfo jobInfo) throws Exception {
		Job job = new Job(new Configuration(), jobName);
		job.setJarByClass(jobInfo.getJarByClass());
		job.setMapperClass(jobInfo.getMapperClass());
		if (jobInfo.getCombinerClass() != null)
			job.setCombinerClass(jobInfo.getCombinerClass());
		job.setReducerClass(jobInfo.getReducerClass());
		job.setOutputKeyClass(jobInfo.getOutputKeyClass());
		job.setOutputValueClass(jobInfo.getOutputValueClass());
		return job;
	}
	
}
