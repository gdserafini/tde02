import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxDriver {
    public static void main(String[] args) throws Exception {
        //if (args.length != 2) {
        //    System.err.println("Uso: MinMaxDriver <entrada> <saida>");
        //    System.exit(2);
        //}

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf, "MinMax Brasil 2016");

        job.setJarByClass(MinMaxDriver.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setReducerClass(MinMaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path("../data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

