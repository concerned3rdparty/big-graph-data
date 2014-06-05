package com.mpcs53112.finalproject.fw;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FWReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long k = context.getCounter(FWJob.Iterations.numberOfIterations).getValue();
		
		System.err.println("I am receiver with the key " + key.toString());
		for (Text value : values) {
			System.err.println("Receiver key: " + key.toString() + ", value: " + value.toString());
		}
		
		String[] keyValues = key.toString().split(",");
		long i = Long.parseLong(keyValues[0]);
		long j = Long.parseLong(keyValues[1]);
		
		long d_ij = 0L, d_ik = 0L, d_kj = 0L;
		final String ij = "" + i + "," + j;
		final String ik = "" + i + "," + k;
		final String kj = "" + k + "," + j;
		for (Text value : values) {
			if (value.toString().startsWith(ij)) {
				d_ij = Long.parseLong(value.toString().split(",")[2]);
			} else if (value.toString().startsWith(ik)) {
				d_ik = Long.parseLong(value.toString().split(",")[2]);
			} else if (value.toString().startsWith(kj)) {
				d_kj = Long.parseLong(value.toString().split(",")[2]);
			}
		}
		long newD = Math.min(d_ij, d_ik + d_kj);
		context.write(key, new Text("" + i + "," + j + "," + newD));
	}	
}