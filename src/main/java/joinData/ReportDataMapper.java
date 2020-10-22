package joinData;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReportDataMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private static final String ZEROVALUE = "0.00";
    private static final String FLIGHTKEY = "1";
    private static final String SEPARATOR = ",";
    private static final String EMPTY = "";
    private static final int COLOMDESTINATIONAIRPORTID = 14;
    private static final int COLOMDELAY = 18;
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split(SEPARATOR);
        if (!data[COLOMDELAY].equals(ZEROVALUE) && !data[COLOMDELAY].equals(EMPTY)) {
            context.write(new TextPair(data[COLOMDESTINATIONAIRPORTID], FLIGHTKEY), new Text(data[COLOMDELAY]));
        }
    }
}