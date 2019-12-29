package com.hadoop.lastfm;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TrackListenerReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
{

	//reduce method
	@Override
	public void reduce(IntWritable trackId,Iterable<IntWritable> userIds,Context context)
	throws IOException,InterruptedException
	{
		Set<Integer> userlist = new HashSet<Integer>();
		
		for(IntWritable ids : userIds)
		{
			userlist.add(ids.get()); //to add unique ids only
		}
		
		IntWritable userIdCount = new IntWritable(userlist.size());
		
		context.write(trackId, userIdCount);
	}
}
