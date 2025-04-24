import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinMaxMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private final static Text keyFixed = new Text("Brasil_2016");

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(";");
        if (fields.length == 10) {
            String country = fields[0].trim();
            String year = fields[1].trim();
            String priceStr = fields[5].trim();

            if (country.equalsIgnoreCase("Brazil") && year.equals("2016") && !priceStr.isEmpty()) {
                try {
                    double price = Double.parseDouble(priceStr);
                    context.write(keyFixed, new DoubleWritable(price));
                } catch (NumberFormatException e) {
                    // ignora linhas com preço inválido
                }
            }
        }
    }
}

