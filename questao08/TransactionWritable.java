import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class TransactionWritable implements Writable {
    private double amount;
    private String rawLine;

    public TransactionWritable() {}

    public TransactionWritable(double amount, String rawLine) {
        this.amount = amount;
        this.rawLine = rawLine;
    }

    public double getAmount() {
        return amount;
    }

    public String getRawLine() {
        return rawLine;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(amount);
        out.writeUTF(rawLine);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        amount = in.readDouble();
        rawLine = in.readUTF();
    }

    @Override
    public String toString() {
        return amount + "\t" + rawLine;
    }
}