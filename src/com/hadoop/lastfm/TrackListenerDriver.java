package com.hadoop.lastfm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrackListenerDriver {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"TrackListnerCount");
		
		job.setJarByClass(TrackListenerDriver.class);
		job.setMapperClass(TrackListenerMapper.class);
		job.setReducerClass(TrackListenerReducer.class);
		
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path("/user/tusharc24249880/lastfm.txt"));
		FileOutputFormat.setOutputPath(job, new Path("javamrout5"));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}

}
