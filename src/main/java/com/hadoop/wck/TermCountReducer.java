package com.hadoop.wck;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class TermCountReducer extends Reducer<Text, LongWritable, Text, Text> {
    private Text newKey = new Text();
    private Text newValue = new Text();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 统计各个属性的边缘概率
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
        newKey.set(key.toString());
        newValue.set(summary);
        context.write(newKey, newValue);
    }
}
