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
public class TwoJob {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String INPUT_PATH = "hdfs:///ceshi/weibo/output1/";
        String OUTPUT_PATH = "hdfs:///ceshi/weibo/output2/";
        Configuration conf = new Configuration();
        FileSystem fileSystem =  FileSystem.get(new URI(OUTPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        Job job2 = Job.getInstance(conf,"微博2");
        job2.setJarByClass(TwoJob.class);

        job2.setMapperClass(TwoMapper.class);
        job2.setCombinerClass(TwoReduce.class);
        job2.setReducerClass(TwoReduce.class);
        //job.setPartitionerClass(FirstPartition.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        //job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job2,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job2,new Path(OUTPUT_PATH));
        boolean f = job2.waitForCompletion(true);
        if (f) {
            System.out.println("执行job2成功");
        }

    }
}
