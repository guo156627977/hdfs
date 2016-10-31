package com.ceb.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		//定义一个累加器
		long count =0;
		for (LongWritable value : values) {
			count += value.get();
		}
		//输出<单词：count>键值对
		context.write(key, new LongWritable(count));
	}
}
