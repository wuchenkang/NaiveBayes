package com.hadoop.wck;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PredictMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private HashMap<Long, HashMap<Long, Double>> classHelper = new HashMap<Long, HashMap<Long, Double>>();
    private HashMap<Long, HashMap<Long, HashMap<Long, Double>>> termHelper = new HashMap<Long, HashMap<Long, HashMap<Long, Double>>>();
    private LongWritable newKey = new LongWritable();
    private Text newValue = new Text();
    private static long total = 0;
    private static long right = 0;

    // 从文件中载入之前步骤得到的各属性边缘概率和条件概率数据
    @Override
    public void setup(Context context) throws IOException {
        BufferedReader classReader = new BufferedReader(new FileReader("output/class/part-r-00000"));
        String row;
        while ((row = classReader.readLine()) != null) {
            // 对每个属性插入HashMap以存储其边缘概率
            long attr = Long.parseLong(row.split("\t")[0]);
            classHelper.put(attr, new HashMap<Long, Double>());

            // 存储属性所有可能值出现的次数
            String[] data = row.split("\t")[1].split(",");
            long total = 0;
            for (String item : data) {
                long val = Long.parseLong(item.split(":")[0]);
                double count = Long.parseLong(item.split(":")[1]) * 1.0;
                classHelper.get(attr).put(val, count);
                total += Long.parseLong(item.split(":")[1]);
            }

            // 除以总出现次数(正则化)得到边缘概率
            for (HashMap.Entry<Long, Double> item : classHelper.get(attr).entrySet()) {
                classHelper.get(attr).put(item.getKey(), item.getValue() / total);
            }
        }
        classReader.close();

        BufferedReader termReader = new BufferedReader(new FileReader("output/term/part-r-00000"));
        while ((row = termReader.readLine()) != null) {
            // 对标签的所有可能值和属性插入HashMap以存储其条件概率
            long label = Long.parseLong(row.split("\t")[0].split(",")[0]);
            long attr = Long.parseLong(row.split("\t")[0].split(",")[1]);
            if (!termHelper.containsKey(label)) {
                termHelper.put(label, new HashMap<Long, HashMap<Long, Double>>());
            }
            if (!termHelper.get(label).containsKey(attr)) {
                termHelper.get(label).put(attr, new HashMap<Long, Double>());
            }

            // 存储给定标签下属性所有可能值出现的次数
            String[] data = row.split("\t")[1].split(",");
            long total = 0;
            for (String item : data) {
                long val = Long.parseLong(item.split(":")[0]);
                double count = Long.parseLong(item.split(":")[1]) * 1.0;
                termHelper.get(label).get(attr).put(val, count);
                total += Long.parseLong(item.split(":")[1]);
            }

            // 除以总出现次数(正则化)得到条件概率，对于未出现的可能值，赋概率值0
            for (HashMap.Entry<Long, Double> item : classHelper.get(attr).entrySet()) {
                if (termHelper.get(label).get(attr).containsKey(item.getKey())) {
                    termHelper.get(label).get(attr).put(item.getKey(), termHelper.get(label).get(attr).get(item.getKey()) / total);
                } else {
                    termHelper.get(label).get(attr).put(item.getKey(), 0.0);
                }
            }
        }
    }

    // 计算每行数据对应的可能标签值的后验概率
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 测试集数据不包含标签，而验证集包含标签
        long[] attrs;
        if (JobScheduler.forTest) {
            attrs = extract(value);
        } else {
            attrs = Arrays.copyOfRange(extract(value), 0, extract(value).length - 1);
        }

        // 根据先验和似然度计算后验
        HashMap<Long, Double> probabilities = new HashMap<Long, Double>();
        double sum = 0;
        for (Map.Entry<Long, Double> entry : classHelper.get((long) attrs.length).entrySet()) {
            double probability = entry.getValue();
            for (int i = 0; i < attrs.length; i++) {
                // 防止测试/验证集中的数据属性值未在训练集中出现导致的错误
                if (termHelper.get(entry.getKey()).get((long) i).get(attrs[i]) != null) {
                    probability *= termHelper.get(entry.getKey()).get((long) i).get(attrs[i]);
                } else {
                    probability = 0;
                }
            }
            probabilities.put(entry.getKey(), probability);
            sum += probability;
        }
        // 归一化处理得到后验
        for (Map.Entry<Long, Double> entry: probabilities.entrySet()){
            if(sum != 0){
                probabilities.put(entry.getKey(), entry.getValue() / sum);
            }
        }

        // 验证集计算准确率
        if (!JobScheduler.forTest) {
            total++;
            long maxIndex = 0;
            double max = 0;
            for (Map.Entry<Long, Double> entry : probabilities.entrySet()) {
                if (entry.getValue() > max) {
                    max = entry.getValue();
                    maxIndex = entry.getKey();
                }
            }
            long real = extract(value)[extract(value).length - 1];
            if (real == maxIndex) {
                right++;
            }
        }

        // 输出预测得到各可能标签的概率信息
        boolean first = true;
        String info = "";
        for (Map.Entry<Long, Double> entry : probabilities.entrySet()) {
            if (first) {
                first = false;
            } else {
                info += ",";
            }
            info += String.valueOf(entry.getKey()) + ":" + String.valueOf(entry.getValue());
        }

        newKey.set(total);
        newValue.set(info);
        context.write(newKey, newValue);
    }

    // 输出准确率
    @Override
    public void cleanup(Context context) {
        System.out.println("Right predict:\t" + right + "/" + total);
        System.out.println(("Accuracy:\t") + right * 1.0 / total);
    }

    // 从Text类中以数组格式提取出数据
    public long[] extract(Text value) {
        String[] valueStr = value.toString().split(",");
        long[] valueLong = new long[valueStr.length];
        for (int i = 0; i < valueStr.length; i++) {
            valueLong[i] = Long.parseLong(valueStr[i]);
        }
        return valueLong;
    }
}
