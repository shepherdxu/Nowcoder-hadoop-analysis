package nowcoder.mr14_skill_collection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SkillCollectionReducer extends Reducer<Text, IntWritable, Text, Text> {
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int totalCollection = 0, count = 0;
        for (IntWritable val : values) {
            totalCollection += val.get();
            count++;
        }
        result.set(totalCollection + "\t" + count);
        context.write(key, result);
    }
}
