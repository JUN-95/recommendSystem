package com.jun.mapreduce.RecommendSystem4;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.jun.mapreduce.RecommendSystem4.Step2.Step2_Mapper;
import com.jun.mapreduce.RecommendSystem4.Step2.Step2_Reducer;

/**
 * 
 * 
 * 	a	刘一,王五,2
	b	陈二,刘一,李四,3
	c	张三,李四,陈二,3
	d	王五,张三,刘一,李四,4
	e	陈二,1
	
 * @author Administrator
 *
 */

public class Step7 {
	
	
	public static boolean run(Configuration conf,Map<String, String> paths,String HOST_URL, String HOST_NAME){
		try {
			FileSystem fs = FileSystem.get(new URI(HOST_URL), conf, HOST_NAME);
			Job job =Job.getInstance(conf);
			job.setJobName("step7");
			job.setJarByClass(Run.class);
			job.setMapperClass(Step7_Mapper.class);
			job.setReducerClass(Step7_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));
			Path outpath=new Path(paths.get("Step7Output"));
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
	
	public static class Step7_Mapper extends Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			//zhangsan,2,a
			String values = value.toString();
			String[] vals = values.split(",");
			Text product = new Text(vals[2]);
			Text person = new Text(vals[0]);
			context.write(product, person);
		}
		
		
		
	}
	
	public static class Step7_Reducer extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			//a 張三,李四2
			int sum = 0;
			String values = "";
			for(Text val: value){
				
				sum += 1;
//				sum = sum + 1;
				if(sum!=1){
					
					values += ",";
				}
				values +=val.toString();
				
			}
			
			values += "," + sum;   
			context.write(key, new Text(values));
		}
		
	}
}
