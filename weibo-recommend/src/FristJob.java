import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by shaohui on 2016/12/17 0017.
 */
public class FristJob {

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {

        String INPUT_PATH = "hdfs:///ceshi/weibo/input/";
        String OUTPUT_PATH = "hdfs:///ceshi/weibo/output1/";
        Configuration conf = new Configuration();
        FileSystem fileSystem =  FileSystem.get(new URI(OUTPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        Job job1 = Job.getInstance(conf,"微博1");
        job1.setJarByClass(FristJob.class);

        job1.setMapperClass(FirstMapper.Map1.class);
        job1.setCombinerClass(FirstReduce.class);
        job1.setReducerClass(FirstReduce.class);
        job1.setPartitionerClass(FirstPartition.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job1,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job1,new Path(OUTPUT_PATH));
        boolean f = job1.waitForCompletion(true);
        if (f) {
            System.out.println("执行job1成功");
        }
    }
}
