package joinData;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReportAirportsMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    private static final String TITLE = "Code,Description";
    private static final String AIRPORTKEY = "0";
    private static final String QUOTATION = "\"";
    private static final String SEPARATOR = "\",\"";
    private static final String EMPTY = "";
    private static final int COLOMAIRPORTCODE = 0;
    private static final int COLOMAIRPORTDESCRIPTION = 1;
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (!line.equals(TITLE)) {
            String[] data = line.split(SEPARATOR);
            context.write(new TextPair(data[COLOMAIRPORTCODE].replace(QUOTATION, EMPTY), AIRPORTKEY),
                    new Text(data[COLOMAIRPORTDESCRIPTION].replace(QUOTATION, EMPTY)));
        }
    }
}