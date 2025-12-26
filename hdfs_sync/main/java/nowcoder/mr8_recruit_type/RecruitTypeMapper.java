package nowcoder.mr8_recruit_type;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RecruitTypeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text typeKey = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.equals("[") || line.equals("]"))
            return;
        if (line.endsWith(","))
            line = line.substring(0, line.length() - 1);
        try {
            JobRecord record = JobRecord.fromJson(line);
            String recruitType = record.getRecruitType();
            if (recruitType != null && !recruitType.isEmpty()) {
                typeKey.set(recruitType);
                context.write(typeKey, one);
            }
        } catch (Exception e) {
        }
    }
}
