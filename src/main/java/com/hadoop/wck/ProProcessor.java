package com.hadoop.wck;

import java.io.*;
import java.util.ArrayList;

class ProProcessor {
    // 创建对应文件夹
    public static void mkdir(String input){
        String[] tempArr = input.split("/");
        String pathStr = "";
        for(int i = 0; i < tempArr.length-1; i++){
            if(i != 0){
                pathStr += "/";
            }
            pathStr += tempArr[i];
        }
        new File(pathStr).mkdirs();
    }

    // 去除用于属性间间隔的多余空白字符
    public static void trim(String input, String output){
        try{
            BufferedReader inputReader = new BufferedReader(new FileReader(input));
            BufferedWriter outputWriter = new BufferedWriter(new FileWriter(output));

            String before, after;
            while((before = inputReader.readLine()) != null){
                after = before.replaceAll(" +", " ").trim();
                outputWriter.write(after+"\n");
            }

            outputWriter.close();
            inputReader.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    // 预处理并划分得到训练集和验证集
    public static void deal(String raw, String train, String validate, double rate){
        try{
            BufferedReader inputReader = new BufferedReader(new FileReader(raw));
            BufferedWriter trainWriter = new BufferedWriter(new FileWriter(train));
            BufferedWriter validateWriter = new BufferedWriter((new FileWriter(validate)));

            String row;
            ArrayList<String> allData = new ArrayList<String>();
            while((row = inputReader.readLine()) != null){
                if(row == ""){
                    continue;
                }

                String attributes[] = row.split(" ");

                // 减少月份属性的类别数目
                attributes[1] = String.valueOf(Integer.parseInt(attributes[1]) / 6);

                // 减少借贷额属性的类别数目
                attributes[3] = String.valueOf(Integer.parseInt(attributes[3]) / 10);

                // 减少年龄属性的类别数目
                attributes[9] = String.valueOf(Integer.parseInt(attributes[9]) / 10);

                // 转换为String
                String temp = "";
                for(int i = 0; i < attributes.length; i++){
                    if(i != 0){
                        temp += ",";
                    }
                    temp += attributes[i];
                }

                allData.add(temp);
            }

            // 得到训练集和验证集
            int boundary = (int)(rate* allData.size());
            for(int i = 0; i < boundary; i++){
                trainWriter.write(allData.get(i) + "\n");
            }
            for(int i = boundary; i < allData.size(); i++){
                validateWriter.write(allData.get(i) + "\n");
            }

            validateWriter.close();
            trainWriter.close();
            inputReader.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void main(String raw, String input, String train, String validate, double rate){
        mkdir(train);
        mkdir(validate);
        trim(raw, input);
        deal(input, train, validate, rate);
    }
}