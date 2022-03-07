package com.zhening;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Zhening Li
 */
public class TrafficBean implements Writable {

    /**
     * 上行流量
     */
    private long upstreamTraffic;
    /**
     * 下行流量
     */
    private long downstreamTraffic;
    /**
     * 总流量
     */
    private long totalTraffic;

    public TrafficBean(){}

    public TrafficBean(long upstreamTraffic,long downstreamTraffic){
        this.upstreamTraffic = upstreamTraffic;
        this.downstreamTraffic = downstreamTraffic;
        this.totalTraffic = upstreamTraffic + downstreamTraffic;
    }

    public void set(long upstreamTraffic, long downstreamTraffic) {
        this.upstreamTraffic = upstreamTraffic;
        this.downstreamTraffic = downstreamTraffic;
        this.totalTraffic = upstreamTraffic + downstreamTraffic;
    }

    public long getUpstreamTraffic() {
        return upstreamTraffic;
    }

    public void setUpstreamTraffic(long upstreamTraffic){
        this.upstreamTraffic = upstreamTraffic;
    }

    public long getDownstreamTraffic() {
        return downstreamTraffic;
    }

    public void setDownstreamTraffic(long downstreamTraffic) {
        this.downstreamTraffic = downstreamTraffic;
    }

    public long getTotalTraffic() {
        return totalTraffic;
    }

    public void setTotalTraffic(long totalTraffic) {
        this.totalTraffic = totalTraffic;
    }

    /**
     * 序列化方法
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upstreamTraffic);
        dataOutput.writeLong(downstreamTraffic);
        dataOutput.writeLong(totalTraffic);
    }

    /**
     * 反序列化方法
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upstreamTraffic = dataInput.readLong();
        downstreamTraffic = dataInput.readLong();
        totalTraffic = dataInput.readLong();
    }

    /**
     * 输出打印
     * @return
     */
    @Override
    public String toString() {
        return upstreamTraffic + "\t" + downstreamTraffic + "\t" + totalTraffic;
    }
}
