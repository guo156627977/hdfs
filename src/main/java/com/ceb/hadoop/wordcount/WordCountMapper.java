package com.ceb.hadoop.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//获取一行文件的
		String line = value.toString();
		//切分这一行的内容为单词数组
		String[] words = StringUtils.split(line, " ");
		//遍历输出<word,1>
		for (String word : words) {
			context.write(new Text(word), new LongWritable(1));
		}
	}
}
