package joinData;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TextPair implements WritableComparable<TextPair> {
    public TextPair() {

    }

    public TextPair(String first, String second) {
        this.first = first;
        this.second = second;
    }

    private String first;
    private String second;

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TextPair textPair = (TextPair) o;
        return first.equals(textPair.first) && second.equals(textPair.getSecond());
    }

    @Override
    public int hashCode() {
        return Objects.hash(first);
    }

    @Override
    public int compareTo(TextPair o) {
        int res = first.compareTo(o.getFirst());
        return res == 0 ? second.compareTo(o.getSecond()) : res;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeUTF(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readUTF();
        second = dataInput.readUTF();
    }
}
