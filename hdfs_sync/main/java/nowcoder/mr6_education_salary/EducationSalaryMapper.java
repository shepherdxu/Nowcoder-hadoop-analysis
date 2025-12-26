package nowcoder.mr6_education_salary;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class EducationSalaryMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text eduKey = new Text();
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
            String edu = record.getEducation();
            Integer avgSalary = record.getAvgMonthlySalary();
            if (edu != null && !edu.isEmpty() && avgSalary != null) {
                eduKey.set(edu);
                salaryValue.set(avgSalary + "," + record.getMinSalary() + "," + record.getMaxSalary() + ",1");
                context.write(eduKey, salaryValue);
            }
        } catch (Exception e) {
        }
    }
}
