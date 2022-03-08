package com.zhening;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author Zhening Li
 */
public class TrafficDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 加载jar包
        job.setJarByClass(TrafficDriver.class);

        // 关联mapper和reducer
        job.setMapperClass(TrafficMapper.class);
        job.setReducerClass(TrafficReducer.class);

        // 设置最终输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TrafficBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TrafficBean.class);

        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 提交任务
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
