import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by shaohui on 2016/12/17 0017.
 */
public class FirstReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable i : values){
            sum = sum + i.get();
        }
        if(key.equals(new Text("count"))){
            System.out.println(key.toString() + "__________" + sum);//输出微博总条数N
        }
        context.write(key,new IntWritable(sum));//统计每个微博id中所有分词出现的次数
    }
}
