package nowcoder.mr11_high_collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MR11: 高收藏岗位分析 - Driver
 * 
 * 使用方法:
 * hadoop jar hdfsapi-1.0-SNAPSHOT.jar
 * nowcoder.mr11_high_collection.HighCollectionDriver <input> <output>
 */
public class HighCollectionDriver {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: HighCollectionDriver <input path> <output path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Nowcoder High Collection Jobs Analysis");
        job.setJarByClass(HighCollectionDriver.class);

        job.setMapperClass(HighCollectionMapper.class);
        job.setReducerClass(HighCollectionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 删除已存在的输出目录
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println("========================================");
            System.out.println("  MR11 高收藏岗位分析完成!");
            System.out.println("========================================");
            System.out.println("高收藏岗位数: " + job.getCounters()
                    .findCounter("HighCollection", "MatchedJobs").getValue());
        }

        System.exit(success ? 0 : 1);
    }
}
