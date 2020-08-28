package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper  extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        // k1 代表的是每一行的行首偏移量  v1代表的是每一行内容
        //对获取到的每一行数据进行切割 把单词切割出来
        String[] words = v1.toString().split(" ");
        for (String word : words) {
            Text k2 = new Text(word);
            LongWritable v2 = new LongWritable(1L);
            context.write(k2, v2);
        }


    }
}
