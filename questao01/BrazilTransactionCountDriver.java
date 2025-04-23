import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BrazilTransactionCountDriver {
    public static void main(String[] args) throws Exception {
        //if (args.length != 2) {
        //    System.err.println("Usage: BrazilTransactionCountDriver <input path> <output path>");
        //    System.exit(-1);
        //}

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf, "Brazil Transaction Count");

        job.setJarByClass(BrazilTransactionCountDriver.class);
        job.setMapperClass(BrazilTransactionCountMapper.class);
        job.setReducerClass(BrazilTransactionCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
