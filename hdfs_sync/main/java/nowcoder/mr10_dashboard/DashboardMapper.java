package nowcoder.mr10_dashboard;

import nowcoder.model.JobRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DashboardMapper extends Mapper<LongWritable, Text, Text, Text> {
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
            int salary = avgSalary != null ? avgSalary : 0;
            int salaryCount = avgSalary != null ? 1 : 0;
            // 格式: 岗位数,薪资总计,有效薪资数
            context.write(new Text("ALL"), new Text("1," + salary + "," + salaryCount));
        } catch (Exception e) {
        }
    }
}
