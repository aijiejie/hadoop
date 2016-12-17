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
public class RunJob {
    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {

        String INPUT_PATH1 = "hdfs:///ceshi/weibo/input/";
        String OUTPUT_PATH1 = "hdfs:///ceshi/weibo/output1/";
        Configuration conf = new Configuration();
        FileSystem fileSystem1 = FileSystem.get(new URI(OUTPUT_PATH1), conf);
        if (fileSystem1.exists(new Path(OUTPUT_PATH1))) {
            fileSystem1.delete(new Path(OUTPUT_PATH1), true);
        }
        Job job1 = Job.getInstance(conf, "微博1");
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
        FileInputFormat.addInputPath(job1, new Path(INPUT_PATH1));
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH1));
        boolean f1 = job1.waitForCompletion(true);
        if (f1){
            System.out.println("执行job1成功");
            String INPUT_PATH2 = "hdfs:///ceshi/weibo/output1/";
            String OUTPUT_PATH2 = "hdfs:///ceshi/weibo/output2/";
            FileSystem fileSystem2 =  FileSystem.get(new URI(OUTPUT_PATH2),conf);
            if (fileSystem2.exists(new Path(OUTPUT_PATH2))){
                fileSystem2.delete(new Path(OUTPUT_PATH2),true);
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
            FileInputFormat.addInputPath(job2,new Path(INPUT_PATH2));
            FileOutputFormat.setOutputPath(job2,new Path(OUTPUT_PATH2));
            boolean f2 = job2.waitForCompletion(true);
            if (f2){
                System.out.println("执行job2成功");
                String INPUT_PATH3 = "hdfs:///ceshi/weibo/output1/";
                String OUTPUT_PATH3 = "hdfs:///ceshi/weibo/output3/";
                FileSystem fileSystem3 =  FileSystem.get(new URI(OUTPUT_PATH3),conf);
                if (fileSystem3.exists(new Path(OUTPUT_PATH3))){
                    fileSystem3.delete(new Path(OUTPUT_PATH3),true);
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
                FileInputFormat.addInputPath(job3,new Path(INPUT_PATH3));
                FileOutputFormat.setOutputPath(job3,new Path(OUTPUT_PATH3));
                boolean f = job3.waitForCompletion(true);
                System.exit(f ? 0 : 1);
            }
        }
    }
}
