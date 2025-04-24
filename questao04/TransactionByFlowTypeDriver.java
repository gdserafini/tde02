import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TransactionByFlowTypeDriver {
    public static void main(String[] args) throws Exception {
        //if (args.length != 2) {
        //    System.err.println("Uso: FlowDriver <entrada> <saida>");
        //    System.exit(2);
        //}

        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf, "Transações por tipo de fluxo");

        job.setJarByClass(TransactionByFlowTypeDriver.class);
        job.setMapperClass(TransactionByFlowTypeMapper.class);
        job.setCombinerClass(TransactionByFlowTypeReducer.class);
        job.setReducerClass(TransactionByFlowTypeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("../data.csv"));
        FileOutputFormat.setOutputPath(job, new Path("output/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

