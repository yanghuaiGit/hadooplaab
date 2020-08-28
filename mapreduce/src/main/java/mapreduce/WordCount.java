package mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * 读取 hello.txt 文件 计算文件中每个单词出现的总次数
 */
public class WordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {


        Configuration entries = new Configuration();
//        entries.set("mapreduce.app-submission.cross-platform","true");
        entries.set("mapreduce.framework.name","yarn");
        entries.set("mapred.jar","/Users/yanghuai/IdeaProjects/myProject/hadooplab/mapreduce/target/mapreduce-1.0-SNAPSHOT.jar");
//        entries.set("yarn.nodemanager.aux-services","mapreduce_shuffle");
        entries.set("yarn.resourcemanager.hostname","v1");
        Job job = Job.getInstance(entries);

        job.setJarByClass(WordCount.class);
//       TextInputFormat 是默认的InputFormat 键是该行在整个文件的偏移量而不是行号 v是该行内容
//       job.setInputFormatClass(TextInputFormat.class);
        //指定输入路径 可以是文件也可以是目录
        FileInputFormat.setInputPaths(job, new Path("hdfs://v1:9000/test.txt"));
        //指定输出路径 只能指定一个不存在的目录
        FileOutputFormat.setOutputPath(job, new Path("/test1.txt"));

        job.setMapperClass(MyMapper.class);

        //如果map的输出类型和reduce的输入类型是一致的 那么只需要调用 job.setOutputKeyClass  job.setOutputValueClass

        //指定k2类型
        job.setMapOutputKeyClass(Text.class);
        //指定v2类型
        job.setMapOutputValueClass(LongWritable.class);

        //由于java泛型的类型擦除导致在运行过程中类型信息并非一直可见 因此hadoop需要明确指定class类
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //启动一个runJar进行提交任务
        job.waitForCompletion(true);//用于提交以前没有提交的过的作业并等待它的完成
//        job.submit();
    }


}
