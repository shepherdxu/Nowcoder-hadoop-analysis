package nowcoder.mr1_city_count;

import nowcoder.util.MySQLUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * MR1: 城市岗位统计 - 完整Driver（带MySQL写入）
 * 
 * 使用方法:
 * hadoop jar hdfsapi-1.0-SNAPSHOT.jar
 * nowcoder.mr1_city_count.CityCountDriverWithMySQL <input> <output>
 */
public class CityCountDriverWithMySQL {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: CityCountDriverWithMySQL <input path> <output path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // 创建 Job
        Job job = Job.getInstance(conf, "Nowcoder City Job Count");
        job.setJarByClass(CityCountDriverWithMySQL.class);

        // 设置 Mapper 和 Reducer
        job.setMapperClass(CityCountMapper.class);
        job.setCombinerClass(CityCountReducer.class);
        job.setReducerClass(CityCountReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入输出路径
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 删除已存在的输出目录
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("已删除旧输出目录: " + outputPath);
        }

        // 提交任务
        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println("\n========================================");
            System.out.println("  MapReduce 任务完成!");
            System.out.println("========================================");

            // 读取输出结果并写入 MySQL
            System.out.println("\n正在将结果写入 MySQL...");
            writeToMySQL(fs, outputPath, conf);

            System.out.println("✓ 数据已成功写入 MySQL!");
        }

        System.exit(success ? 0 : 1);
    }

    /**
     * 读取 HDFS 输出结果并写入 MySQL
     */
    private static void writeToMySQL(FileSystem fs, Path outputPath, Configuration conf) throws Exception {
        Map<String, Integer> cityCountMap = new HashMap<>();

        // 读取 part-r-* 文件
        Path resultFile = new Path(outputPath, "part-r-00000");

        if (fs.exists(resultFile)) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(resultFile), "UTF-8"))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        String city = parts[0];
                        int count = Integer.parseInt(parts[1].trim());
                        cityCountMap.put(city, count);
                    }
                }
            }
        }

        System.out.println("读取到 " + cityCountMap.size() + " 条城市统计记录");

        // 批量写入 MySQL
        MySQLUtil.batchInsertCityJobCount(cityCountMap);

        // 打印 Top 10
        System.out.println("\nTop 10 城市:");
        cityCountMap.entrySet().stream()
                .sorted((a, b) -> b.getValue() - a.getValue())
                .limit(10)
                .forEach(e -> System.out.println("  " + e.getKey() + ": " + e.getValue()));
    }
}
