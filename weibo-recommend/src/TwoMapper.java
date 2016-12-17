import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 统计DF
 * Created by shaohui on 2016/12/17 0017.
 */
public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        if(! fs.getPath().getName().contains("part-r-00003")){
            String[] v = value.toString().trim().split("\t");
            if (v.length >= 2){
                String[] w_id = v[0].split("_");
                if (w_id.length >=2){
                    String w = w_id[0];
                    context.write(new Text(w),new IntWritable(1));//用来统计出现关键字的微博条数DF
                }
            }
        }
    }
}
