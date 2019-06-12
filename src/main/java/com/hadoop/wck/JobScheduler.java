package com.hadoop.wck;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobScheduler {
    public static boolean forTest = false;
    public static void main(String[] args) throws Exception {
        // 创建配置信息
        Configuration conf = new Configuration();

        // 预处理
        ProProcessor.main("raw/raw.txt", "raw/input.txt", "input/train/train.txt", "input/validate/validate.txt", 0.8);

        // 统计各属性边缘概率数据
        Job classCountJob = Job.getInstance(conf, "Class Count");
        classCountJob.setJarByClass(JobScheduler.class);
        classCountJob.setMapperClass(ClassCountMapper.class);
        classCountJob.setReducerClass(ClassCountReducer.class);
        classCountJob.setOutputKeyClass(LongWritable.class);
        classCountJob.setOutputValueClass(Text.class);
        classCountJob.setMapOutputKeyClass(LongWritable.class);
        classCountJob.setMapOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(classCountJob, new Path("input/train"));
        FileOutputFormat.setOutputPath(classCountJob, new Path("output/class"));
        classCountJob.waitForCompletion(true);

        // 统计各属性在给定标签下的条件概率数据
        Job termCountJob = Job.getInstance(conf, "Term Count");
        termCountJob.setJarByClass(JobScheduler.class);
        termCountJob.setMapperClass(TermCountMapper.class);
        termCountJob.setReducerClass(TermCountReducer.class);
        termCountJob.setOutputKeyClass(Text.class);
        termCountJob.setOutputValueClass(Text.class);
        termCountJob.setMapOutputKeyClass(Text.class);
        termCountJob.setMapOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(termCountJob, new Path("input/train"));
        FileOutputFormat.setOutputPath(termCountJob, new Path("output/term"));
        termCountJob.waitForCompletion(true);

        // 使用验证集进行验证
        Job predictJob = Job.getInstance(conf, "Predict");
        predictJob.setJarByClass(JobScheduler.class);
        predictJob.setMapperClass(PredictMapper.class);
        predictJob.setReducerClass(PredictReducer.class);
        predictJob.setOutputKeyClass(LongWritable.class);
        predictJob.setOutputValueClass(Text.class);
        predictJob.setMapOutputKeyClass(LongWritable.class);
        predictJob.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(predictJob, new Path("input/validate"));
        FileOutputFormat.setOutputPath(predictJob, new Path("output/predict"));
        predictJob.waitForCompletion(true);
    }
}
