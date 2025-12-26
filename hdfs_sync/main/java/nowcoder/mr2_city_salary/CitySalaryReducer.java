package nowcoder.mr2_city_salary;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * MR2: 城市薪资统计 - Reducer
 * 输出: <城市, 平均薪资|最低|最高|数量>
 */
public class CitySalaryReducer extends Reducer<Text, Text, Text, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long totalSalary = 0;
        int minSalary = Integer.MAX_VALUE;
        int maxSalary = 0;
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length >= 4) {
                totalSalary += Long.parseLong(parts[0]);
                minSalary = Math.min(minSalary, Integer.parseInt(parts[1]));
                maxSalary = Math.max(maxSalary, Integer.parseInt(parts[2]));
                count += Integer.parseInt(parts[3]);
            }
        }

        if (count > 0) {
            long avgSalary = totalSalary / count;
            result.set(avgSalary + "\t" + minSalary + "\t" + maxSalary + "\t" + count);
            context.write(key, result);
        }
    }
}
