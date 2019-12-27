package com.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object,Text,Text,LongWritable>
{

    @Override
    public void map(Object key,Text value,Context context) 
    		throws IOException,InterruptedException
    {
    	Text mykey = new Text();
    	String line = value.toString();
    	StringTokenizer token = new StringTokenizer(line);
    	
    	while(token.hasMoreTokens())
    	{
    		mykey.set(token.nextToken());
    		context.write(mykey, new LongWritable(1));
    	}
    
    }
	
}
