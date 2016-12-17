package sort;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.NotionalTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Created by shaohui on 2016/12/16 0016.
 */
public class reducesort {
    public static class Map1 extends Mapper<LongWritable,Text,Text,IntWritable> {

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            HanLP.Config.ShowTermNature = false;
            String str = new String(value.getBytes(),"UTF-8") ;//将text类型的value以UTF-8编码读取
            //context.write(new Text("直接读取String：" + str), new IntWritable(1));//直接输出整句
            //String str = new String(value.getBytes(),"GBK") ;////将text类型的value以GBK编码读取
            Segment segment = HanLP.newSegment();//新建分词器
            //segment.enableJapaneseNameRecognize(true);//开启日本人名识别

            List<Term> sgm = segment.seg(str);//自动分词
            CoreStopWordDictionary.add("一直");
            CoreStopWordDictionary.add("觉得");
            CoreStopWordDictionary.apply(sgm);
            for (Term s : sgm) {
                context.write(new Text("分词：" + s ), new IntWritable(1));
            }


            List<String> keywordList = HanLP.extractKeyword(str, 3);//提取关键字
            for (String keyword : keywordList){//提取关键词
                context.write(new Text("关键词：" + keyword),new IntWritable(1));
            }


            for (List<Term> sentence : NotionalTokenizer.seg2sentence(str)){//去除停用词并自动切分，输出切分后的整句
                context.write(new Text("自动断句加去除停用词：" + sentence), new IntWritable(1));
            }

            List<Term> realwords = NotionalTokenizer.segment(str);//实词分词并去除停用词
            for (Term realword :realwords){
                context.write(new Text("实词分词并去除停用词： " + realword),new IntWritable(1));
            }

        }
    }

    static class sumreduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum =0;
            for (IntWritable v :values) {
                sum += v.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {

        String INPUT_PATH = "hdfs:///ceshi/hanlpTest/input/";
        String OUTPUT_PATH = "hdfs:///ceshi/hanlpTest/output/";
        Path tempDir = new Path("hdfs:///ceshi/hanlpTest/temp/"); //定义一个临时目录
        Configuration conf = new Configuration();
        FileSystem fileSystem =  FileSystem.get(new URI(INPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        Job job = Job.getInstance(conf,"hanlpTest");
        job.setJarByClass(reducesort.class);
        job.setMapperClass(reducesort.Map1.class);
        job.setReducerClass(reducesort.sumreduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job,tempDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        if(job.waitForCompletion(true))
        {
            Job sortJob = Job.getInstance(conf, "sort");
            sortJob.setJarByClass(reducesort.class);
            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(SequenceFileInputFormat.class);
                /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
            sortJob.setMapperClass(InverseMapper.class);
                /*将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。*/
            sortJob.setNumReduceTasks(1);
            FileOutputFormat.setOutputPath(sortJob, new Path(OUTPUT_PATH));
            sortJob.setOutputKeyClass(IntWritable.class);
            sortJob.setOutputValueClass(Text.class);
                /*Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。
                 * 因此我们实现了一个 IntWritableDecreasingComparator 类,　
                 * 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序*/
            sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
        }
    }
}
