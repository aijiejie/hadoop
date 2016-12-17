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
public class LastJob {

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String INPUT_PATH = "hdfs:///ceshi/weibo/output1/";
        String OUTPUT_PATH = "hdfs:///ceshi/weibo/output3/";
        Configuration conf = new Configuration();
        FileSystem fileSystem =  FileSystem.get(new URI(OUTPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        Job job3 = Job.getInstance(conf,"微博3");
        job3.setJarByClass(TwoJob.class);
        job3.addCacheFile(new Path("hdfs:///ceshi/weibo/output1/part-r-00003").toUri());
        job3.addCacheFile(new Path("hdfs:///ceshi/weibo/output2/part-r-00000").toUri());

        job3.setMapperClass(LastMapper.class);
        job3.setCombinerClass(LastReduce.class);
        job3.setReducerClass(LastReduce.class);
        //job.setPartitionerClass(FirstPartition.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job3,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job3,new Path(OUTPUT_PATH));
        boolean f = job3.waitForCompletion(true);
        if (f) {
            System.out.println("执行job3成功");
        }

    }
}
