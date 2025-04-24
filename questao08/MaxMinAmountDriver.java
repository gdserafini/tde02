import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxMinAmountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(conf, "Max and Min Amount by Year and Country");
        job.setJarByClass(MaxMinAmountDriver.class);

        job.setMapperClass(MaxMinAmountMapper.class);
        job.setReducerClass(MaxMinAmountReducer.class);

        job.setMapOutputKeyClass(YearCountryPair.class);
        job.setMapOutputValueClass(TransactionWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("../data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}