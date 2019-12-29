package com.hadoop.lastfm;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrackListenerMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
	
	
	public class LFConstants
	{
		public static final int user_id=0;
		public static final int track_id=1;
		public static final int shared=2;
		public static final int radio=3;
		public static final int skipped=4;
		
	}
	
	IntWritable trackId = new IntWritable();
	IntWritable userId = new IntWritable();
	
	
	@Override
	public void map(Object key,Text value,Context context) throws IOException,InterruptedException
	{
		String[] div = value.toString().split("[|]");
		trackId.set(Integer.parseInt(div[LFConstants.track_id]));
		userId.set(Integer.parseInt(div[LFConstants.user_id]));
		context.write(trackId, userId);
		
	}
	
	

}
