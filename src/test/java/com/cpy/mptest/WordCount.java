package com.cpy.mptest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.validation.Validator;
import java.io.IOException;

/**
 * 字符统计
 */
public class WordCount {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//         1 创建配置对象
        Configuration config = new Configuration();
//       2  初始化作业对象 工作空间
        Job job = Job.getInstance(config);
//        3 设置当前作业
        job.setJarByClass(WordCount.class);
//        设置当前作业的mapclass\
        job.setMapperClass(WordCountMapper.class);
//        设置作业的reduce类
        job.setReducerClass(WordCountReduce.class);
//        设置map类的输出类型（对应maper）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        设置reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        设置数据的读写格式
        Path inputpath = new Path("C:\\Users\\小宇\\IdeaProjects\\mp\\src\\test\\java\\com\\cpy\\word\\abc.txt");
        Path outputpath = new Path("C:\\Users\\小宇\\IdeaProjects\\mp\\src\\test\\java\\com\\cpy\\word\\output");
        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,outputpath);
        boolean result = job.waitForCompletion(true);
        System.out.println(result? "chenggong": "shibai");
    }

    /*map任务
    * */
    private  static  class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            System.out.println("行偏移量" + key);
            System.out.println("value" + value);
//            super.map(key, value, context);
        }
    }

    /*reduce 任务*/
    private static class WordCountReduce extends Reducer<Text, IntWritable ,Text , IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws
                IOException, InterruptedException {
//            super.reduce(key, values, context);
        }
    }

}

