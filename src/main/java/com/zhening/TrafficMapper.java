package com.zhening;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Zhening Li
 */
public class TrafficMapper extends Mapper<LongWritable, Text, Text, TrafficBean> {
    private final Text phoneNumber = new Text();
    private final TrafficBean traffic  = new TrafficBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 提取一行数据转换成 String
        String data = value.toString();
        // 切分字段
        String[] fields = data.split("\t");
        // 抓取手机号
        phoneNumber.set(fields[1]);
        // 抓取上下行流量
        long upstreamTraffic = Long.parseLong(fields[fields.length-3]);
        long downstreamTraffic = Long.parseLong(fields[fields.length-2]);
        traffic.set(upstreamTraffic,downstreamTraffic);
        // 写入content
        context.write(phoneNumber,traffic);
    }
}
