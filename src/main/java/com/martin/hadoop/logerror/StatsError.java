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

public class StatsError {

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
					context.write(ErrorKey, one);
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

		Job job = new Job(conf, "statics application error");
		//
		job.setJarByClass(StatsError.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(ErrorSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
