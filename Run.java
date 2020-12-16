package com.jun.mapreduce.RecommendSystem4;


import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class Run {
	private static final String HOST_NAME = "root";
	private static final String HOST_URL = "hdfs://hadoop01:8020";
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		
//		conf.set("mapreduce.app-submission.corss-paltform", "true");
//		conf.set("mapreduce.framework.name", "local");
		
		//所有mr的输入和输出目录定义在map集合中
		Map<String, String> paths = new HashMap<String, String>();
		paths.put("Step1Input", "hdfs://hadoop01:8020/jun/data/users.txt");
		paths.put("Step1Output", "hdfs://hadoop01:8020/jun/output/RecommendSystem3/step1/");
		paths.put("Step2Input", paths.get("Step1Output"));
		paths.put("Step2Output", "hdfs://hadoop01:8020/jun/output/RecommendSystem3/step2/");
		paths.put("Step3Input", paths.get("Step2Output"));
		paths.put("Step3Output", "hdfs://hadoop01:8020/jun/output/RecommendSystem3/step3/");
		paths.put("Step4Input1", paths.get("Step3Output"));
		paths.put("Step4Output", "hdfs://hadoop01:8020/jun/output/RecommendSystem3/step4/");
		paths.put("Step5Input", paths.get("Step4Output"));
		paths.put("Step5Output", "hdfs://hadoop01:8020/jun/output/RecommendSystem3/step5/");
		paths.put("Step6Input", paths.get("Step5Output"));
		paths.put("Step6Output", "hdfs://hadoop01:8020/jun/output/RecommendSystem3/step6/");
		paths.put("Step7Output", "hdfs://hadoop01:8020/jun/output/RecommendSystem3/step7/");
		paths.put("Step4Input2", paths.get("Step7Output"));
		paths.put("Step8Input1", paths.get("Step2Output"));
		paths.put("Step8Input2", paths.get("Step4Output"));
		paths.put("Step8Output", "hdfs://hadoop01:8020/jun/output/RecommendSystem3/step8/");
//		System.out.println("1111111111111111111" +paths.get("Step8Input1") );
//		System.out.println("2222222222222222222" +paths.get("Step8Input2") );
//		System.out.println("outoutoutotutoutotu" + paths.get("Step8Output"));
//		System.out.println(paths.get("Step4Input2"));
		Step1.run(conf, paths,HOST_URL,HOST_NAME);
//		Step2.run(conf, paths,HOST_URL,HOST_NAME);
//		Step3.run(conf, paths,HOST_URL,HOST_NAME);
//		Step4.run(conf, paths,HOST_URL,HOST_NAME);
////		Step5.run(conf, paths,HOST_URL,HOST_NAME);
////		Step6.run(conf, paths,HOST_URL,HOST_NAME);
//		Step7.run(conf, paths,HOST_URL,HOST_NAME);
//		Step8.run(conf, paths,HOST_URL,HOST_NAME);
	}

//	public static Map<String, Integer> R = new HashMap<String, Integer>();
//	//数字代表事件相应的得分
//	static {
//		R.put("click", 1);
//		R.put("collect", 2);
//		R.put("cart", 3);
//		R.put("alipay", 4);
//	}
}
