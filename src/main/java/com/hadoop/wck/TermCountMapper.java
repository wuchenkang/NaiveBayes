package com.hadoop.wck;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TermCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text newKey = new Text();
    private LongWritable newValue = new LongWritable();

    // 将单行数据根据属性个数拆分成多个(标签, 属性, 值)对，以便Reducer中对给定标签下各个属性的条件概率进行统计
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        long[] values = extract(value);
        for(int i = 0; i < values.length - 1; i++){
            String str = String.valueOf(values[values.length-1]) + "," + String.valueOf(i);
            newKey.set(str);
            newValue.set(values[i]);
            context.write(newKey, newValue);
        }
    }

    // 从Text类中以数组格式提取出数据
    public long[] extract(Text value) {
        String[] valueStr = value.toString().split(",");
        long[] valueLong = new long[valueStr.length];
        for(int i = 0; i < valueStr.length; i++) {
            valueLong[i] = Long.parseLong(valueStr[i]);
        }
        return valueLong;
    }
}
