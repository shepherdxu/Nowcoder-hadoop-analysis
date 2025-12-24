package nowcoder.mr1_city_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MR1: 城市岗位统计 - Driver
 * 
 * 使用方法:
 * hadoop jar hdfsapi-1.0-SNAPSHOT.jar nowcoder.mr1_city_count.CityCountDriver
 * <input> <output>
 * 
 * 示例:
 * hadoop jar hdfsapi-1.0-SNAPSHOT.jar nowcoder.mr1_city_count.CityCountDriver \
 * /user/hadoop/nowcoder_jobs/dt=20251223 \
 * /user/hadoop/output/city_count
 */
public class CityCountDriver {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: CityCountDriver <input path> <output path>");
            System.err.println(
                    "Example: CityCountDriver /user/hadoop/nowcoder_jobs/dt=20251223 /user/hadoop/output/city_count");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // 创建 Job
        Job job = Job.getInstance(conf, "Nowcoder City Job Count");
        job.setJarByClass(CityCountDriver.class);

        // 设置 Mapper 和 Reducer
        job.setMapperClass(CityCountMapper.class);
        job.setCombinerClass(CityCountReducer.class); // 使用 Combiner 优化
        job.setReducerClass(CityCountReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交任务并等待完成
        boolean success = job.waitForCompletion(true);

        // 打印统计信息
        if (success) {
            System.out.println("========================================");
            System.out.println("  城市岗位统计任务完成!");
            System.out.println("========================================");
            System.out.println("输入路径: " + args[0]);
            System.out.println("输出路径: " + args[1]);
            System.out.println("Map 输入记录数: " + job.getCounters()
                    .findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue());
            System.out.println("Reduce 输出记录数: " + job.getCounters()
                    .findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS").getValue());
        }

        System.exit(success ? 0 : 1);
    }
}
