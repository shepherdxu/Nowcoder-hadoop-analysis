package nowcoder.mr11_high_collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MR11: 高收藏岗位分析 - Reducer
 * 输入: <城市, [收藏数1, 收藏数2, ...]>
 * 输出: <城市, 高收藏岗位数|总收藏数>
 */
public class HighCollectionReducer extends Reducer<Text, IntWritable, Text, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;
        int totalCollection = 0;

        for (IntWritable val : values) {
            count++;
            totalCollection += val.get();
        }

        // 输出格式: 高收藏岗位数|总收藏数
        result.set(count + "\t" + totalCollection);
        context.write(key, result);
    }
}
