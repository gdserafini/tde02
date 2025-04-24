import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransactionByCategoryMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private boolean isHeader = true;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (isHeader && line.contains("Category")) {
            isHeader = false;
            return;
        }

        String[] fields = line.split(";");
        if (fields.length < 3) return;

        String category = fields[9].trim();
        if (!category.isEmpty()) {
            context.write(new Text(category), ONE);
        }
    }
}
