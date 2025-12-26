package nowcoder.mr3_skill_count;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * MR3: 技能热度统计 - Mapper
 */
public class SkillCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text skillKey = new Text();
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
            String[] skills = record.getSkillArray();
            for (String skill : skills) {
                if (skill != null && !skill.trim().isEmpty()) {
                    skillKey.set(skill.trim());
                    context.write(skillKey, one);
                }
            }
        } catch (Exception e) {
            context.getCounter("SkillCount", "ParseErrors").increment(1);
        }
    }
}
