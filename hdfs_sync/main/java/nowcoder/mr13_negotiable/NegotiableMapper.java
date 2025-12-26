package nowcoder.mr13_negotiable;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MR13: 薪资面议比例分析 - Mapper
 * 输出: <城市, 是否面议(1/0)>
 */
public class NegotiableMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text cityKey = new Text();
    private final Text countValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();

        if (line.isEmpty() || line.equals("[") || line.equals("]")) {
            return;
        }

        if (line.endsWith(",")) {
            line = line.substring(0, line.length() - 1);
        }

        try {
            JobRecord record = JobRecord.fromJson(line);
            String city = record.getEffectiveCity();

            // 格式: 面议数,总数
            String val = record.isNegotiable() ? "1,1" : "0,1";

            cityKey.set(city);
            countValue.set(val);
            context.write(cityKey, countValue);
        } catch (Exception e) {
            context.getCounter("Negotiable", "ParseErrors").increment(1);
        }
    }
}
