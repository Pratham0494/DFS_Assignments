package assignment;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Q1 {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key , Text value , Context context)throws IOException , InterruptedException
		{
			String line = value.toString();
			String[] words = line.split(",");
			
			for(String w:words)
			{
				context.write(new Text(w), new IntWritable(1));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key , Iterable<IntWritable> value , Context context)throws IOException , InterruptedException
		{
			int sum = 0;
			for(IntWritable val:value)
			{
				sum = sum + val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	
	public static void main(String args[])throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf , "wordcount");
		
		job.setJarByClass(Q1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job , new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
	}
}
