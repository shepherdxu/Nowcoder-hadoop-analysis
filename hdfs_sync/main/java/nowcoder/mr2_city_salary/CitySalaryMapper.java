package nowcoder.mr2_city_salary;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * MR2: 城市薪资统计 - Mapper
 * 输出: <城市, 薪资信息>
 */
public class CitySalaryMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text cityKey = new Text();
    private final Text salaryValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty() || line.equals("[") || line.equals("]"))
            return;
        if (line.endsWith(","))
            line = line.substring(0, line.length() - 1);

        try {
            JobRecord record = JobRecord.fromJson(line);
            Integer avgSalary = record.getAvgMonthlySalary();

            if (avgSalary != null) {
                String city = record.getEffectiveCity();
                // 格式: 平均薪资,最低,最高,计数
                salaryValue.set(avgSalary + "," + record.getMinSalary() + "," + record.getMaxSalary() + ",1");
                cityKey.set(city);
                context.write(cityKey, salaryValue);
            }
        } catch (Exception e) {
            context.getCounter("CitySalary", "ParseErrors").increment(1);
        }
    }
}
