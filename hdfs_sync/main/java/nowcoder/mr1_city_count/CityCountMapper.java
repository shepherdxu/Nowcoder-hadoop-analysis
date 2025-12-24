package nowcoder.mr1_city_count;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MR1: 城市岗位统计 - Mapper
 * 输入: JSON 行
 * 输出: <城市, 1>
 */
public class CityCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text cityKey = new Text();
    private final IntWritable one = new IntWritable(1);

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

        // 解析 JSON
        try {
            JobRecord record = JobRecord.fromJson(line);
            String city = record.getEffectiveCity();

            if (city != null && !city.isEmpty()) {
                cityKey.set(city);
                context.write(cityKey, one);
            }
        } catch (Exception e) {
            // 解析失败，跳过该记录
            context.getCounter("CityCount", "ParseErrors").increment(1);
        }
    }
}
