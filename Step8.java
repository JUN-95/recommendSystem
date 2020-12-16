package com.jun.mapreduce.RecommendSystem4;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.jun.mapreduce.RecommendSystem4.Step4.Step4_Mapper;
import com.jun.mapreduce.RecommendSystem4.Step4.Step4_Reducer;

public class Step8 {
	
//	private static HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
	public static boolean run(Configuration conf, Map<String, String> paths,String HOST_URL, String HOST_NAME) {
		try {
			FileSystem fs = FileSystem.get(new URI(HOST_URL), conf, HOST_NAME);
			Job job = Job.getInstance(conf);
			job.setJobName("step4");
			job.setJarByClass(Run.class);
			job.setMapperClass(Step8_Mapper.class);
			job.setReducerClass(Step8_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			// FileInputFormat.addInputPath(job, new)
			// Path(paths.get("Step4Input")));
//			System.out.println(paths.get("Step4Input1"));
//			System.out.println(paths.get("Step4Input2"));
			FileInputFormat.setInputPaths(job, new Path[] { new Path(paths.get("Step8Input1")),
							new Path(paths.get("Step8Input2")) });
			Path outpath = new Path(paths.get("Step8Output"));
			System.out.println("1111111111111" + paths.get("Step8Input1")+"\n"+ "2222222222222"+ paths.get("Step8Input2"));
			System.out.println("oooooooooooooooo" + outpath);
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
	
	
	public static class Step8_Mapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private String flag;
		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();// 判断读的数据集	
			System.out.println("flagflagflagflag--------" + flag);
		}
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if(flag.equals("step4")){
//				a:b	0.4082482904638631
//				a:d	0.7071067811865475
				
				String line = value.toString();
				String[] vals = line.split("\t");
				String thing_ab = vals[0];
				String thing_a = thing_ab.split(":")[0];
				String thing_b = thing_ab.split(":")[1];
				Double similar_score = Double.parseDouble(vals[1]);
				Text k = new Text(thing_a);
//				System.out.println("BBBBBBBBBBBBBBBBB" + similar_score.toString());
//				System.out.println("bbbbbbbbbbbbbbbbb" + thing_b);
//				System.out.println("aaaaaaaaaaaaaaaa" + thing_a);
				Text v = new Text("B_" + thing_b + ":" + similar_score.toString());
				//	a   B_b:0.4082482904638631
				
				context.write(k, v);
			}
			
			if (flag.equals("step2")) {
				//刘一	a:3,b:4,d:5
				
				String line = value.toString();
				String[] vals = line.split("\t");
				String userId = vals[0];
				String thing_score = vals[1];
//				System.out.println("AAAAAAAAAAAAAAAAA" + "A_" + thing_score);
				context.write(new Text(userId), new Text("A_" + thing_score));
				//刘一		A_a:3,b:4,d:5
				
			}
		}
	}
	
	public static class Step8_Reducer extends Reducer<Text, Text, Text, Text>{
		
		Map<String, String> hashMapA = new HashMap<String, String>();
		Map<String, String> hashMapB = new HashMap<String, String>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for(Text line: values){
				String new_line = line.toString();
				
				if(new_line.startsWith("B_")){
					//	a   B_b:0.4082482904638631,d:0.7071067811865475
					
					String[] vals = new_line.split("_");
//					String[] thing_a_b_similar_score =  vals[1].split(":");
//					String thing_a =  thing_a_b_similar_score[0];
					String[] thing_b_similar_scores =  vals[1].split(",");
					for(int i=0;i<thing_b_similar_scores.length;i++){
						String[] thing_b_similar_score = thing_b_similar_scores[i].split(":");
						
						String thing_b =  thing_b_similar_score[0];
						String similar_score = thing_b_similar_score[1];
						hashMapB.put(key.toString()+":"+thing_b, similar_score);
						//a:b,0.1
					}
					
					Iterator<String> keys = hashMapB.keySet().iterator();
					while(keys.hasNext()){
						String keybb =  keys.next();
//						System.out.println("keybbbbbb:------" + keybb);
					}
				}

				//刘一		A_a:3,b:4,d:5
				else if(new_line.startsWith("A_")){
					
					String[] vals = new_line.split("_");
					String thing_score_line = vals[1];
//					System.out.println("key.toString()"+key.toString()+"-------thing_score_line: "+ thing_score_line);
					hashMapA.put(key.toString(), thing_score_line);
					
//					System.out.println("kkkkkkkkkkkkkkkkkkkkkk-----------"+key);
//					System.out.println("hhhhhhhhhhhhhhhhhh"+hashMapA.get(key.toString()));
					String[] thing_scores = hashMapA.get(key.toString()).split(",");
					for(int i=0;i<thing_scores.length;i++){
						
						String[] thing_score = thing_scores[i].split(":");
						String thing = thing_score[0];
						int score = Integer.parseInt(thing_score[1]);
						Iterator<String> keys = hashMapB.keySet().iterator();
						while(keys.hasNext()){
							
							String keyab = keys.next();
							String thing_a = keyab.split(":")[0];
							String thing_b = keyab.split(":")[1];
							if(thing.equals(thing_a)){
								for(int j=0;j<thing_scores.length;j++){
									String[] thing_score2 = thing_scores[j].split(":");
									String thing2 = thing_score2[0];
									if(thing2 != thing_b){
										Double res = Double.parseDouble(hashMapB.get(keyab)) * score;
										context.write(key, new Text(keyab+":" + res.toString()));
									}
							}

							
						}
					}
					}
				}

			
			}
		}
	}
}
