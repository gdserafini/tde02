import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgExportBrazilMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    private boolean isHeader = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (isHeader && (line.contains("Country") || line.contains("country_or_area"))) {
            isHeader = false;
            return;
        }

        String[] fields = line.split(";");
        if (fields.length < 6) return;

        String country = fields[0].trim();
        String yearStr = fields[1].trim();
        String flow = fields[4].trim();
        String priceStr = fields[5].trim();

        try {
            if (country.equalsIgnoreCase("Brazil") && flow.equalsIgnoreCase("Export")) {
                int year = Integer.parseInt(yearStr);
                double price = Double.parseDouble(priceStr.replace(",", ""));
                context.write(new IntWritable(year), new DoubleWritable(price));
            }
        } catch (Exception e) {
            // ignora erros de parsing
        }
    }
}