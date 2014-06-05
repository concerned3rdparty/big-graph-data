package com.mpcs53112.finalproject.fw;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

public class FWMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		long k = context.getCounter(FWJob.Iterations.numberOfIterations).getValue();
		long numberOfNodes = context.getCounter(FWJob.Numbers.numberOfNodes).getValue();
		
		System.err.println("I am a mapper and got this value: " + value.toString());
		System.err.println("I see k as " + k + " and number of nodes as " + numberOfNodes);
		
		String[] inputValues = value.toString().split(",");
		long i = Long.parseLong(inputValues[0]);
		long j = Long.parseLong(inputValues[1]);
		
		context.write(new Text("" + i + "," + j), value);
			
		if (i == k) {
			for (int x = 0; x < numberOfNodes; x++) {
				if (x != i) {
					context.write(new Text("" + x + "," + j), value);
				}
			}
		}
		
		if (j == k && j != i) {
			for (int x = 0; x < numberOfNodes; x++) {
				if (x != j) {
					context.write(new Text("" + i + "," + x), value);
				}
			}
		}		
	}
}
