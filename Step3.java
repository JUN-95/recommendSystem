package com.jun.mapreduce.RecommendSystem4;


import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
/**
 * 对物品组合列表进行计数，建立物品的同现矩阵
	刘一		a:3,b:4,d:5
	张三		c:5,d:3
	李四		b:3,c:4,d:5
	王五		a:3,d:5
	陈二		b:5,c:4,e:3



	a:b	1
	a:d	2
	b:a	1
	b:c	2
	b:d	2
	b:e	1
	c:b	2
	c:d	2
	c:e	1
	d:a	2
	d:b	2
	d:c	2
	e:b	1
	e:c	1


 * @author root
 *
 */
public class Step3 {
	 private final static Text K = new Text();
     private final static IntWritable V = new IntWritable(1);
	
	public static boolean run(Configuration conf,Map<String, String> paths,String HOST_URL, String HOST_NAME){
		try {
			FileSystem fs = FileSystem.get(new URI(HOST_URL), conf, HOST_NAME);
			Job job =Job.getInstance(conf);
			job.setJobName("step3");
			job.setJarByClass(Run.class);
			job.setMapperClass(Step3_Mapper.class);
			job.setReducerClass(Step3_Reducer.class);
			job.setCombinerClass(Step3_Reducer.class);
//			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step3Input")));
			Path outpath=new Path(paths.get("Step3Output"));
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
	
	 static class Step3_Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			
			//u3244	i469:1,i498:1,i154:1,i73:1,i162:1,
			
			//刘一	a:3,b:4,d:5
			String[]  tokens=value.toString().split("\t");
			String[] thing =tokens[1].split(",");
			for (int i = 0; i < thing.length; i++) {
				String thingA = thing[i].split(":")[0];
				for (int j = 0; j < thing.length; j++) {
					if(i!=j){
						String thingB = thing[j].split(":")[0];
						K.set(thingA+":"+thingB);
						context.write(K, V);

					}
				}
			}
			
		}
	}
	
	 
	 static class Step3_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{

			protected void reduce(Text key, Iterable<IntWritable> i,
					Context context)
					throws IOException, InterruptedException {
				int sum =0;
				for(IntWritable v :i ){
					sum =sum+v.get();
				}
				V.set(sum);
				context.write(key, V);
			}
		}
	 
}
