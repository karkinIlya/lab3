package joinData;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    public GroupingComparator() {
        super(TextPair.class, true);
    }

    @Override
    public int compare (WritableComparable o1, WritableComparable o2) {
        final TextPair textPair1 = (TextPair)o1;
        final TextPair textPair2 = (TextPair)o2;
        return textPair1.getFirst().compareTo(textPair2.getFirst());
    }
}
