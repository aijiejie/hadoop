
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Created by shaohui on 2016/12/15 0015.
 */
public class FirstMapper  {


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

    static class sumreduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum =0;
            for (IntWritable v :values) {
                sum += v.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

//    public static class rdc extends Reducer<Text,IntWritable,Text,IntWritable>{
//        @Override
//        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            int sum = 0;
//            for ()
//        }
//    }

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {

        String INPUT_PATH = "hdfs:///ceshi/hanlpTest/input/";
        String OUTPUT_PATH = "hdfs:///ceshi/hanlpTest/output/";
        Configuration conf = new Configuration();
        FileSystem fileSystem =  FileSystem.get(new URI(INPUT_PATH),conf);
        if (fileSystem.exists(new Path(OUTPUT_PATH))){
            fileSystem.delete(new Path(OUTPUT_PATH),true);
        }
        Job job = Job.getInstance(conf,"hanlpTest");
        job.setJarByClass(FirstMapper.class);
        job.setMapperClass(FirstMapper.Map1.class);
        job.setReducerClass(sumreduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job,new Path(OUTPUT_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
