import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxMinAmountMapper extends Mapper<LongWritable, Text, YearCountryPair, TransactionWritable> {
    private boolean isHeader = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (isHeader && (line.contains("Country") || line.contains("country_or_area"))) {
            isHeader = false;
            return;
        }

        String[] fields = line.split(";");
        if (fields.length < 9) return;

        try {
            String country = fields[0].trim();
            int year = Integer.parseInt(fields[1].trim());
            String amountStr = fields[8].trim();
            double amount = Double.parseDouble(amountStr.replace(",", ""));

            YearCountryPair keyPair = new YearCountryPair(year, country);
            TransactionWritable valueWritable = new TransactionWritable(amount, line);

            context.write(keyPair, valueWritable);
        } catch (Exception e) {
            // Ignora erros de parsing
        }
    }
}