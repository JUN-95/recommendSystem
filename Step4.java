package com.jun.mapreduce.RecommendSystem4;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;
import org.junit.Test;



/** 
 * 
 * 
 * @author root
 *
 *	a	刘一,王五,2
	b	陈二,刘一,李四,3
	c	张三,李四,陈二,3
	d	王五,张三,刘一,李四,4
	e	陈二,1
 *
 *
 *
 *
 *	a:b	1
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


	output:
		a:b	0.4082482904638631
		a:d	0.7071067811865475
		b:a	0.4082482904638631
		b:e	0.5773502691896258
		b:c	0.6666666666666666
		b:d	0.5773502691896258
		c:b	0.6666666666666666
		c:e	0.5773502691896258
		c:d	0.5773502691896258
		d:a	0.7071067811865475
		d:b	0.5773502691896258
		d:c	0.5773502691896258
		e:b	0.5773502691896258
		e:c	0.5773502691896258

 */
public class Step4 {
	private static HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
	public static boolean run(Configuration conf, Map<String, String> paths,String HOST_URL, String HOST_NAME) {
		try {
			FileSystem fs = FileSystem.get(new URI(HOST_URL), conf, HOST_NAME);
			Job job = Job.getInstance(conf);
			job.setJobName("step4");
			job.setJarByClass(Run.class);
			job.setMapperClass(Step4_Mapper.class);
			job.setReducerClass(Step4_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			// FileInputFormat.addInputPath(job, new
			// Path(paths.get("Step4Input")));
//			System.out.println(paths.get("Step4Input1"));
//			System.out.println(paths.get("Step4Input2"));
			FileInputFormat.setInputPaths(job,
					new Path[] { new Path(paths.get("Step4Input1")),
							new Path(paths.get("Step4Input2")) });
			Path outpath = new Path(paths.get("Step4Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
		private String flag;

		//每个maptask，初始化时调用一次
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();// 判断读的数据集
			
//			System.out.println(flag + "**********************");
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t]").split(value.toString());
			
			if (flag.equals("step3")) {
				String[] v1 = tokens[0].split(":");
				String thingID1 = v1[0];
				String thingID2 = v1[1];
				String num = tokens[1];
				Text k = new Text(thingID1);
				Text v = new Text("A:" + thingID1+ ":"+ thingID2 + "," + num);// A:i109,1

				context.write(k, v);

				
			} else if (flag.equals("step7")) {
				
				
				//new: a	刘一,王五,2
				
				
				String thingID = value.toString().split("\t")[0];
//				System.out.println("----------------"+thingID);
//				System.out.println("=============" + value.toString().split("\t")[1]);
				String num = value.toString().split("\t")[1].split(",")[value.toString().split("\t")[1].split(",").length-1];
//				System.out.println("####################"+num);
				hashMap.put(thingID, Integer.parseInt(num));
				Text k = new Text(thingID); 
				Text v = new Text("B:" + num);
					
				context.write(k, v);
			}
		}
	}

	static class Step4_Reducer extends Reducer<Text, Text, Text, DoubleWritable> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text line : values) {
				String val = line.toString();
				if(val.startsWith("A:")){
					//A:a:b 1
					String thingId1 = val.split(":")[1];  
//					System.out.println("---------"+thingId1);
					String thingId2 = val.split(":")[2].split(",")[0];
//					System.out.println("##############"+val.split(":")[2]);
					String num2 = val.split(":")[2].split(",")[1];
					String k = thingId1 + ":" + thingId2;
					System.out.println(hashMap.get(thingId1));
					double res = Double.parseDouble(num2) / Math.sqrt(hashMap.get(thingId1) * hashMap.get(thingId2));
					System.out.println("-----------------"+res);
					context.write(new Text(k),new DoubleWritable(res));
					
				}
			}

		}
	}
	
}
