package nowcoder.mr5_education_count;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class EducationCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Text eduKey = new Text();
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
            String edu = record.getEducation();
            if (edu != null && !edu.isEmpty()) {
                eduKey.set(edu);
                context.write(eduKey, one);
            }
        } catch (Exception e) {
        }
    }
}
