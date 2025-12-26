package nowcoder.mr15_activity_salary;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class ActivitySalaryMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text activityKey = new Text();
    private final Text salaryValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || line.equals("[") || line.equals("]"))
            return;
        if (line.endsWith(","))
            line = line.substring(0, line.length() - 1);
        try {
            JobRecord record = JobRecord.fromJson(line);
            Integer avgSalary = record.getAvgMonthlySalary();
            if (avgSalary != null) {
                String activityType = record.isActive() ? "活跃" : "非活跃";
                activityKey.set(activityType);
                salaryValue.set(avgSalary + "," + record.getMinSalary() + "," + record.getMaxSalary() + ",1");
                context.write(activityKey, salaryValue);
            }
        } catch (Exception e) {
        }
    }
}
