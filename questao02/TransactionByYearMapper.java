import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransactionByYearMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private boolean isHeader = true;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (isHeader && line.contains("Year")) {
            isHeader = false;
            return;
        }

        String[] fields = line.split(";");
        if (fields.length < 2) return;

        String year = fields[1].trim();
        if (!year.isEmpty()) {
            context.write(new Text(year), ONE);
        }
    }
}
