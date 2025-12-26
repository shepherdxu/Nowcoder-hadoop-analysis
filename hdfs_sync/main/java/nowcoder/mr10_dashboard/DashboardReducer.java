package nowcoder.mr10_dashboard;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class DashboardReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalJobs = 0, totalSalary = 0, salaryCount = 0;
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length >= 3) {
                totalJobs += Long.parseLong(parts[0]);
                totalSalary += Long.parseLong(parts[1]);
                salaryCount += Long.parseLong(parts[2]);
            }
        }
        long avgSalary = salaryCount > 0 ? totalSalary / salaryCount : 0;
        // 输出: 总岗位数 平均薪资 有效薪资岗位数
        context.write(new Text("total_jobs"), new Text(String.valueOf(totalJobs)));
        context.write(new Text("avg_salary"), new Text(String.valueOf(avgSalary)));
        context.write(new Text("valid_salary_jobs"), new Text(String.valueOf(salaryCount)));
    }
}
