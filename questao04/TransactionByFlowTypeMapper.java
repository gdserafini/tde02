import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransactionByFlowTypeMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text flowType = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length == 10) {
            String flow = fields[4].trim(); // flow type
            if (!flow.isEmpty()) {
                flowType.set(flow);
                context.write(flowType, one);
            }
        }
    }
}

