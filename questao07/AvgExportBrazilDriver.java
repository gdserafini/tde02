import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgExportBrazilDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "Average Export Price - Brazil");
        job.setJarByClass(AvgExportBrazilDriver.class);

        job.setMapperClass(AvgExportBrazilMapper.class);
        job.setReducerClass(AvgExportBrazilReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path("../data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}