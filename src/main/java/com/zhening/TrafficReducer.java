package com.zhening;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Zhening Li
 */
public class TrafficReducer extends Reducer<Text,TrafficBean,Text,TrafficBean> {
    @Override
    protected void reduce(Text key, Iterable<TrafficBean> values, Context context) throws IOException, InterruptedException{
        long sumUpstreamTraffic = 0;
        long sumDownstreamTraffic = 0;
        for(TrafficBean value:values){
            sumUpstreamTraffic += value.getUpstreamTraffic();
            sumDownstreamTraffic += value.getDownstreamTraffic();
        }
        context.write(key,new TrafficBean(sumUpstreamTraffic,sumDownstreamTraffic));
    }
}
