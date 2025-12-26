package nowcoder.mr9_internship;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class InternshipReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalSalary = 0;
        int count = 0;
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length >= 2) {
                totalSalary += Long.parseLong(parts[0]);
                count += Integer.parseInt(parts[1]);
            }
        }
        long avgSalary = count > 0 ? totalSalary / count : 0;
        result.set(count + "\t" + avgSalary);
        context.write(key, result);
    }
}
