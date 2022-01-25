package cn.doitedu.spark.course;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

public class MapReduce_MapSide_JOIN {

    public static class JoinMapper extends Mapper<LongWritable, Text,Text, NullWritable>{
        HashMap<String, String> 小表数据 = new HashMap<>();
        /**
         * maptask在处理正常的流表数据之前，会执行一次该方法
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new FileReader("wc4.txt"));
            String line = null;
            while((line=br.readLine())!=null){
                String[] split = line.split(",");
                小表数据.put(split[0],split[1]+","+split[2]);
            }
        }

        // 流表（大表）数据
        // 1,zs,doit29,80
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String id  = split[0];
            // 根据id,到已经加载好的hashmap中取查找小表信息
            String extraInfo = 小表数据.get(id);
            context.write(new Text(id+","+split[1]+","+split[2]+","+split[3]+","+extraInfo),NullWritable.get());
        }
    }


    public static void main(String[] args) throws  Exception {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setMapperClass(JoinMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,"hdfs://doit01:8020/data/input/");


        // 给每个task的运行时容器，发送一份缓存文件
        job.addCacheFile(new URI("hdfs://doit01:8020/mr_cache/wc4.txt"));

        // 提交任务
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }

}
