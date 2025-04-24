import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double max = Double.MAX_VALUE;
        double min = Double.MIN_VALUE;

        for (DoubleWritable val : values) {
            double price = val.get();
            if (price < min) min = price;
            if (price > max) max = price;
        }

        context.write(key, new Text("Min: " + min + ", Max: " + max));
    }
}
