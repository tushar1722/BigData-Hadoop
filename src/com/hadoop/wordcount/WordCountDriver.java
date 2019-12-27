package com.hadoop.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		
		JobConf conf = new JobConf();
		Job job = new Job(conf,"wordcount");
		
		job.setJarByClass(WordCountDriver.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("/user/tusharc24249880/Word.txt"));
		FileOutputFormat.setOutputPath(job, new Path("javamrout2"));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
		

	}

}
