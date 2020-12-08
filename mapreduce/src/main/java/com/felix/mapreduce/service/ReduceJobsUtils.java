package com.felix.mapreduce.service;

/**
 * @ClassName : ReduceJobsUtils
 * @Description :
 * @Author : felix
 * @Date: 2020-12-07 16:45
 */

import java.io.IOException;

import javax.annotation.PostConstruct;

import com.felix.mapreduce.mapper.WordCountMap;
import com.felix.mapreduce.reducer.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


/**
 * Map/Reduce工具类
 * @author linhaiy
 * @date 2019.05.18
 */
@Component
public class ReduceJobsUtils {

    @Value("${hdfs.path}")
    private String path;

    private static String hdfsPath;

    /**
     * 获取HDFS配置信息
     *
     * @return
     */
    public static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsPath);
        configuration.set("mapred.job.tracker", hdfsPath);
        // 运行在yarn的集群模式
        // configuration.set("mapreduce.framework.name", "yarn");
        // 这个配置是让main方法寻找该机器的mr环境
        // configuration.set("yarn.resourcemanmager.hostname", "node1");
        return configuration;
    }

    /**
     * 获取单词统计的配置信息
     *
     * @param jobName
     * @param inputPath
     * @param outputPath
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void getWordCountJobsConf(String jobName, String inputPath, String outputPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConfiguration();
        Job job = Job.getInstance(conf, jobName);

        job.setMapperClass(WordCountMap.class);
        job.setCombinerClass(WordCountReduce.class);
        job.setReducerClass(WordCountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 小文件合并设置
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 最大分片
        CombineTextInputFormat.setMaxInputSplitSize(job, 4 * 1024 * 1024);
        // 最小分片
        CombineTextInputFormat.setMinInputSplitSize(job, 2 * 1024 * 1024);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.waitForCompletion(true);
    }

    @PostConstruct
    public void getPath() {
        hdfsPath = this.path;
    }

    public static String getHdfsPath() {
        return hdfsPath;
    }
}
