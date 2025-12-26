package nowcoder.mr11_high_collection;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MR11: 高收藏岗位分析 - Mapper
 * 过滤 collection_count >= 50 的岗位
 * 输出: <城市, 收藏数>
 */
public class HighCollectionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text cityKey = new Text();
    private final IntWritable collectionValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        // 跳过空行和数组边界
        if (line.isEmpty() || line.equals("[") || line.equals("]")) {
            return;
        }

        // 移除行尾逗号
        if (line.endsWith(",")) {
            line = line.substring(0, line.length() - 1);
        }

        try {
            JobRecord record = JobRecord.fromJson(line);

            // 只处理高收藏岗位 (>=50)
            if (record.isHighCollection()) {
                String city = record.getEffectiveCity();
                cityKey.set(city);
                collectionValue.set(record.getCollectionCount());
                context.write(cityKey, collectionValue);

                context.getCounter("HighCollection", "MatchedJobs").increment(1);
            }
        } catch (Exception e) {
            context.getCounter("HighCollection", "ParseErrors").increment(1);
        }
    }
}
