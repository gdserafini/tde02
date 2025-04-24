import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxMinAmountReducer extends Reducer<YearCountryPair, TransactionWritable, Text, Text> {

    @Override
    protected void reduce(YearCountryPair key, Iterable<TransactionWritable> values, Context context)
            throws IOException, InterruptedException {

        TransactionWritable min = null;
        TransactionWritable max = null;

        for (TransactionWritable t : values) {
            TransactionWritable current = new TransactionWritable(t.getAmount(), t.getRawLine());

            if (min == null || current.getAmount() < min.getAmount()) {
                min = current;
            }
            if (max == null || current.getAmount() > max.getAmount()) {
                max = current;
            }
        }

        if (min != null) {
            context.write(new Text(key.toString() + "\tMIN"), new Text(min.getRawLine()));
        }
        if (max != null) {
            context.write(new Text(key.toString() + "\tMAX"), new Text(max.getRawLine()));
        }
    }
}