package nowcoder.mr13_negotiable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MR13: 薪资面议比例分析 - Driver
 * 
 * 使用方法:
 * hadoop jar hdfsapi-1.0-SNAPSHOT.jar nowcoder.mr13_negotiable.NegotiableDriver
 * <input> <output>
 */
public class NegotiableDriver {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: NegotiableDriver <input path> <output path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Nowcoder Salary Negotiable Ratio Analysis");
        job.setJarByClass(NegotiableDriver.class);

        job.setMapperClass(NegotiableMapper.class);
        job.setReducerClass(NegotiableReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        boolean success = job.waitForCompletion(true);

        if (success) {
            System.out.println("========================================");
            System.out.println("  MR13 薪资面议比例分析完成!");
            System.out.println("========================================");
        }

        System.exit(success ? 0 : 1);
    }
}
