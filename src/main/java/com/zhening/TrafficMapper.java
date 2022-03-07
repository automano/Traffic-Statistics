package com.zhening;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Zhening Li
 */
public class TrafficMapper extends Mapper<LongWritable, Text, Text, TrafficBean> {
    private Text phoneNumber = new Text();
    private TrafficBean traffic  = new TrafficBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 提取一行数据转换成 String
        String data = value.toString();
        // 切分字段
        String[] fields = data.split("\t");
        // 抓取手机号

    }
}
