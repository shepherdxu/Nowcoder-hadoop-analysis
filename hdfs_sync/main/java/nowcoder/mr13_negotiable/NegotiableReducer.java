package nowcoder.mr13_negotiable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MR13: 薪资面议比例分析 - Reducer
 * 输入: <城市, ["1,1", "0,1", ...]>
 * 输出: <城市, 总数|面议数|面议比例>
 */
public class NegotiableReducer extends Reducer<Text, Text, Text, Text> {

    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int negotiableCount = 0;
        int totalCount = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length == 2) {
                negotiableCount += Integer.parseInt(parts[0]);
                totalCount += Integer.parseInt(parts[1]);
            }
        }

        double ratio = totalCount > 0 ? (negotiableCount * 100.0 / totalCount) : 0;

        // 输出格式: 总数 面议数 面议比例%
        result.set(totalCount + "\t" + negotiableCount + "\t" + String.format("%.2f", ratio));
        context.write(key, result);
    }
}
