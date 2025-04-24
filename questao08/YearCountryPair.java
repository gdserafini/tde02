import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class YearCountryPair implements WritableComparable<YearCountryPair> {
    private int year;
    private String country;

    public YearCountryPair() {}

    public YearCountryPair(int year, String country) {
        this.year = year;
        this.country = country;
    }

    public int getYear() {
        return year;
    }

    public String getCountry() {
        return country;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeUTF(country);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        country = in.readUTF();
    }

    @Override
    public int compareTo(YearCountryPair o) {
        int result = Integer.compare(this.year, o.year);
        if (result != 0) return result;
        return this.country.compareTo(o.country);
    }

    @Override
    public int hashCode() {
        return year * 163 + country.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof YearCountryPair) {
            YearCountryPair other = (YearCountryPair) obj;
            return year == other.year && country.equals(other.country);
        }
        return false;
    }

    @Override
    public String toString() {
        return year + "\t" + country;
    }
}