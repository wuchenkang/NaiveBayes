package com.hadoop.wck;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PredictReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    // 不做处理，直接输出
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text value: values){
            context.write(key, value);
        }
    }
}
