package com.martin.hadoop.logerror;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.ErrorMsg;

import net.sf.json.JSONObject;

public class StatsError {
	private static Logger logger = Logger.getLogger(StatsError.class);

	/**
	 * @author guan.xianchun Mapper区: StatsError程序 Map 类 Mapper<KEYIN, VALUEIN,
	 *         KEYOUT, VALUEOUT>: | | | | 输入key类型 输入value类型 输出key类型 输出value类型
	 */
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static String ERROR = "ERROR";
		private static int start_index = 24;
		private static Text ErrorKey = new Text(ERROR);
		private static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException {
			String line = value.toString();
			if (line == null || line.length() < start_index + ERROR.length()) {
				return;
			}
			String error = line.substring(start_index, start_index + ERROR.length());
			if (ERROR.equals(error)) {
				try {
					String errorData = line.substring(start_index+ERROR.length()).trim();
					logger.info(errorData);
					JSONObject jsonData = null;
					int endIndex = errorData.indexOf(",");
					if (endIndex==-1) {
						return;
					}
					int msg_index = errorData.indexOf("Message");
					String errorMsg = "";
					if (msg_index !=-1) {
						errorMsg = errorData.substring(msg_index+9, errorData.indexOf(",", msg_index));
					}
					errorData = errorData.substring(0, endIndex)+"}";
					try {
						jsonData = JSONObject.fromObject(errorData);
					} catch (Exception e) {
						e.printStackTrace();
						return;
					}
					if (jsonData.containsKey("Code")) {
						context.write(new Text(jsonData.getString("Code")+"_"+errorMsg), one);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static class ErrorSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// 输出结果：总次数
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			try {
				context.write(key, result);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(key + ":" + result);
		}

	}

	public static void main(String[] args)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: statics application error <in> <out>");
			System.exit(2);
		}
		// 获取配置信息
		Configuration conf = new Configuration();
		conf.set("mapred.jar", "C:/Users/Administrator/Downloads/abcloudbackup/StatsError.jar");
		conf.set("tmpjars","hdfs://master:9000/libjars/json-lib-2.4.jar,hdfs://master:9000/libjars/ezmorph-1.0.6.jar");
		//创建任务对象 设置JAR类
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "statics application error");
		
		job.setJarByClass(StatsError.class);
		//设置输入输出目录
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//指定MR类
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(ErrorSumReducer.class);
		//指定输出结果类类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
