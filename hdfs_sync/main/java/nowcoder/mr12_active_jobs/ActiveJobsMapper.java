package nowcoder.mr12_active_jobs;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ActiveJobsMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text cityKey = new Text();
    private final Text infoValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.equals("[") || line.equals("]"))
            return;
        if (line.endsWith(","))
            line = line.substring(0, line.length() - 1);
        try {
            JobRecord record = JobRecord.fromJson(line);
            if (record.isActive()) {
                String city = record.getEffectiveCity();
                Integer avgSalary = record.getAvgMonthlySalary();
                int salary = avgSalary != null ? avgSalary : 0;
                cityKey.set(city);
                infoValue.set(salary + ",1");
                context.write(cityKey, infoValue);
            }
        } catch (Exception e) {
        }
    }
}
