package com.hadoop.partition;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PartitionerEx {
	public static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private static Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			System.out.println("In Mapper");
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				
				word.set(itr.nextToken());
				context.write(word, one);
				
			}
		}
		
	}
	public static class MyReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
		private IntWritable result = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);

		}
		
	}
	public static class MyPartitioner extends Partitioner<Text, IntWritable>{

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			
				System.out.println(key.toString());
				if(key.toString().equals("hadoop")){
					return 0;
				}else if(key.toString().equals("simple")){
					return 1;
				}else{
					System.out.println("in here");
					return 2;
					
				}
			
		}
		
	}
	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException{
		System.out.println("In here main");
		
		try{
			
	        
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Word Count example");
	
			job.setJarByClass(PartitionerEx.class);
			job.setMapperClass(MyMapper.class);
			job.setNumReduceTasks(3);
			job.setPartitionerClass(MyPartitioner.class);
			job.setReducerClass(MyReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
	  
			FileInputFormat.addInputPath(job, new Path(args[0]));
	
			FileSystem fs = FileSystem.get(conf);
	
			if (fs.exists(new Path(args[1]))) {
				fs.delete(new Path(args[1]), true);
			}
	
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.out.println("Before Job");
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}

}
