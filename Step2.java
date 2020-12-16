package com.jun.mapreduce.RecommendSystem4;


import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
	
	用户对物品的评分
	刘一		a:3,b:4,d:5
	张三		c:5,d:3
	李四		b:3,c:4,d:5
	王五		a:3,d:5
	陈二		b:5,c:4,e:3
 * @author root
 *
 */
public class Step2 {

	
	public static boolean run(Configuration conf,Map<String, String> paths,String HOST_URL, String HOST_NAME){
		try {
			FileSystem fs = FileSystem.get(new URI(HOST_URL), conf, HOST_NAME);
			Job job =Job.getInstance(conf);
			job.setJobName("step2");
			job.setJarByClass(Run.class);
			job.setMapperClass(Step2_Mapper.class);
			job.setReducerClass(Step2_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
			Path outpath=new Path(paths.get("Step2Output"));
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
	
	 static class Step2_Mapper extends Mapper<LongWritable, Text, Text, Text>{

		 //如果使用：用户+物品，同时作为输出key，更优
		 //刘一,3,a
		protected void map(LongWritable key, Text value,
				Context context)
				throws IOException, InterruptedException {
			String[]  tokens=value.toString().split(",");
			String user=tokens[0];
			String score =tokens[1];
			String thing =tokens[2];
			Text k= new Text(user);
			
			Text v = new Text(thing + ":" + score);
			//map输出的是Integer类型
		
			context.write(k, v);
			//u2625    i161:1（thing：score）
		}
	}
	
	 
	 static class Step2_Reducer extends Reducer<Text, Text, Text, Text>{

			protected void reduce(Text key, Iterable<Text> i,
					Context context)
					throws IOException, InterruptedException {
				Map<String, Integer> r =new HashMap<String, Integer>();
				for(Text value :i){
					String[] vs =value.toString().split(":");
					String thing=vs[0];
					Integer score=Integer.parseInt(vs[1]);
					score = ((Integer) (r.get(thing)==null?  0:r.get(thing))).intValue() + score;
					r.put(thing,score);
				}
				StringBuffer sb =new StringBuffer();
				for(Entry<String, Integer> entry :r.entrySet() ){
					sb.append(entry.getKey()+":"+entry.getValue().intValue()+",");
				}
				//删掉末尾的逗号
				if(sb.toString().endsWith(",")){
					sb.deleteCharAt(sb.length()-1);
				}
				context.write(key,new Text(sb.toString()));
			}
		}
}
