package com.hadoop.wck;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class ClassCountReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
    private LongWritable newKey = new LongWritable();
    private Text newValue = new Text();

    @Override
    public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 统计单个属性的各个可能值出现的次数
        HashMap<Long, Long> map = new HashMap<Long, Long>();
        for(LongWritable value: values){
            if(map.containsKey(value.get())) {
                map.put(value.get(), map.get(value.get()) + 1L);
            }else {
                map.put(value.get(), 1L);
            }
        }

        // 转换成单行Text格式以写入文件
        String summary = "";
        boolean first = true;
        for(HashMap.Entry<Long, Long> entry: map.entrySet()) {
            if(first){
                first = false;
            }else{
                summary += ",";
            }
            summary += String.valueOf(entry.getKey()) + ":" + String.valueOf(entry.getValue());
        }

        newKey.set(key.get());
        newValue.set(summary);
        context.write(newKey, newValue);
    }
}
