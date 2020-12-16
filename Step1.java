package com.jun.mapreduce.RecommendSystem4;


import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 去重复行
 * @author root
 *
 *	刘一,3,a
	刘一,4,b
	刘一,5,d
	陈二,5,b
	陈二,4,c
	陈二,3,e
	张三,5,c
	张三,3,d
	李四,3,b
	李四,4,c
	李四,5,d
	王五,3,a
	王五,5,d

 *
 */
public class Step1 {

	
	public static boolean run(Configuration conf,Map<String, String> paths,String HOST_URL, String HOST_NAME){
		try {
			FileSystem fs = FileSystem.get(new URI(HOST_URL), conf, HOST_NAME);
			Job job =Job.getInstance(conf);
			job.setJobName("step1");
			job.setJarByClass(Step1.class);
			job.setMapperClass(Step1_Mapper.class);
			job.setReducerClass(Step1_Reducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));
			Path outpath=new Path(paths.get("Step1Output"));
			if(fs.exists(outpath)){
				fs.delete(outpath,true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			
			boolean f= job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	 static class Step1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

				context.write(value, NullWritable.get());

		}
	}
	
	 
	 static class Step1_Reducer extends Reducer<Text, IntWritable, Text, NullWritable>{
		 	//上面的value作为key，reduce相同的key为一组，即可做到去重去行
			protected void reduce(Text key, Iterable<IntWritable> i, Context context)
					throws IOException, InterruptedException {
				context.write(key,NullWritable.get());
			}
		}
}
