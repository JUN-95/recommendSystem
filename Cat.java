package com.jun.mapreduce.RecommendSystem4;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Cat {
	private static final String HOST_NAME = "root";
//	private static final String HOST_URL = "hdfs://hadoop01:8020";
//	private static final String outputPathStep1 = "hdfs://hadoop01:8020/jun/output/RecommSys";
//	private static final String outputPathStep3 = "hdfs://hadoop01:8020/jun/output/RecommSysStep3";
//	private static final String outputPathStep4 = "hdfs://hadoop01:8020/jun/output/RecommSysStep4";
//	private static final String outputPathStep5 = "hdfs://hadoop01:8020/jun/output/RecommSysStep5";
//----------------------------------------
	private static final String HOST_URL = "hdfs://hadoop02:8020";
	private static final String outputPathStep1 = "hdfs://hadoop02:8020/jun/output/RecommendSystem3/step1/";
	private static final String outputPathStep2 = "hdfs://hadoop02:8020/jun/output/RecommendSystem3/step2/";
	private static final String outputPathStep3 = "hdfs://hadoop02:8020/jun/output/RecommendSystem3/step3/";
	private static final String outputPathStep4 = "hdfs://hadoop02:8020/jun/output/RecommendSystem3/step4/";
	private static final String outputPathStep7 = "hdfs://hadoop02:8020/jun/output/RecommendSystem3/step7/";
	private static final String outputPathStep8 = "hdfs://hadoop02:8020/jun/output/RecommendSystem3/step8/";
	private static FileSystem fs;
	
	//查看文件内容
	public void getContext() throws IllegalArgumentException, IOException, InterruptedException, URISyntaxException{

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(HOST_URL), conf, HOST_NAME);
		
//		FSDataInputStream inputStream = fs.open(new Path(outputPathStep1 + "part-r-00000"));
//		FSDataInputStream inputStream = fs.open(new Path(outputPathStep2 + "part-r-00000"));
//		FSDataInputStream inputStream = fs.open(new Path(outputPathStep3 + "part-r-00000"));
//		FSDataInputStream inputStream = fs.open(new Path(outputPathStep4 + "part-r-00000"));
//		FSDataInputStream inputStream = fs.open(new Path(outputPathStep7 + "part-r-00000"));
		FSDataInputStream inputStream = fs.open(new Path(outputPathStep8 + "part-r-00000"));
		String context = inputStreamToString(inputStream, "utf-8");
		System.out.println("查询开始");
		System.out.println(context);
	}

	/**
	 * 把输入流转换为指定编码的字符
	 *
	 * @param inputStream 输入流
	 * @param encode      指定编码类型
	 * @throws UnsupportedEncodingException 
	 */
	private String inputStreamToString(FSDataInputStream inputStream, String encode) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		
		if(encode == null || ("".equals(encode))){   // 判断encode是否为空
			encode = "utf-8";
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, encode));
		StringBuilder builder = new StringBuilder();
		String str = "";
		try {
			while((str = reader.readLine()) != null){
				builder.append(str).append("\n");
			}
			return builder.toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}
	
	public static void main(String args[]) throws IllegalArgumentException, IOException, InterruptedException, URISyntaxException {
		new Cat().getContext();
	}
}
