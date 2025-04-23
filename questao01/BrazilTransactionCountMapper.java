import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BrazilTransactionCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private boolean isHeader = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (isHeader && line.contains("Country;")) {
            isHeader = false;
            return;
        }

        String[] fields = line.split(";");
        if (fields.length != 10) return;

        if (fields[0].equalsIgnoreCase("Brazil")) {
            context.write(new Text("Brazil"), ONE);
        }
    }
}
