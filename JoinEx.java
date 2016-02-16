package com.hadoop.joinex;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.partition.PartitionerEx;


public class JoinEx {

	public static class MyCustMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		private static Text custKey = new Text();
		private static Text custVal = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
		 
			String[] str = value.toString().split(",");
		
			custKey.set(str[0]);
			custVal.set("cx\t"+str[1]);
			try{
				
			
				context.write(custKey, custVal);
			}catch(Exception ex){
				ex.printStackTrace();
			}
			
		
			
		}
		
	}
	public static class MyTransMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			 String[] str = value.toString().split(",");
		
			 context.write(new Text(str[0]), new Text("tx\t"+str[1]));
		}
		
	}
	public static class MyReducer extends Reducer<Text, Text,Text, Text>{
		private Text result = new Text();

		protected void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

		  
            double sum =0.0;
            String name="";
			for (Text val : values) {
				
				String[] parts = val.toString().split("\t");
			
				if(parts[0].equals("cx")){
					
					name = parts[1];
				
				}else{
					sum += Double.parseDouble(parts[1]);
				
				}
			}
          
			result.set(new Text(name+" "+sum));
			
			context.write(key, result);

		}
		
	}
	
	public static void main(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException{
		
		
		try{
			
	        
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Word Count example");
	
			job.setJarByClass(PartitionerEx.class);
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MyCustMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MyTransMapper.class);

			
			job.setReducerClass(MyReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
	  
	
			FileSystem fs = FileSystem.get(conf);
	
			if (fs.exists(new Path(args[2]))) {
				fs.delete(new Path(args[2]), true);
			}
	
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
	 
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}

}
