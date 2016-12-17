import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by shaohui on 2016/12/17 0017.
 */
public class LastReduce extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String s = "";
        for (Text v :values){
            s += v.toString();
            s +="\t" ;
        }
        context.write(key,new Text(s));
    }
}
