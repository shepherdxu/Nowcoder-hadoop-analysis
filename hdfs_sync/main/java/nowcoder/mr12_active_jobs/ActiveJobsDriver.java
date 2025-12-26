package nowcoder.mr12_active_jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ActiveJobsDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ActiveJobsDriver <input> <output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MR12 Active Jobs Stats");
        job.setJarByClass(ActiveJobsDriver.class);
        job.setMapperClass(ActiveJobsMapper.class);
        job.setReducerClass(ActiveJobsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1])))
            fs.delete(new Path(args[1]), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
