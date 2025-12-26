package nowcoder.mr14_skill_collection;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SkillCollectionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text skillKey = new Text();
    private final IntWritable collectionValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.equals("[") || line.equals("]"))
            return;
        if (line.endsWith(","))
            line = line.substring(0, line.length() - 1);
        try {
            JobRecord record = JobRecord.fromJson(line);
            if (record.isHighCollection()) {
                String[] skills = record.getSkillArray();
                int collection = record.getCollectionCount();
                for (String skill : skills) {
                    if (skill != null && !skill.trim().isEmpty()) {
                        skillKey.set(skill.trim());
                        collectionValue.set(collection);
                        context.write(skillKey, collectionValue);
                    }
                }
            }
        } catch (Exception e) {
        }
    }
}
