import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgYearBRMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text year = new Text();
    private DoubleWritable price = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length == 10) {
            String country = fields[0].trim();
            String yearStr = fields[1].trim();
            String priceStr = fields[5].trim();

            if (country.equalsIgnoreCase("Brazil") && !priceStr.isEmpty()) {
                try {
                    double priceVal = Double.parseDouble(priceStr);
                    year.set(yearStr);
                    price.set(priceVal);
                    context.write(year, price);
                } catch (NumberFormatException e) {
                    // ignora linhas com preço inválido
                }
            }
        }
    }
}

