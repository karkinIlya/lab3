package sparkJoinData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReportApp {
    public static final String APPNAME = "Report airports";
    public static final String AIRPORTIDFILE = "L_AIRPORT_ID.csv";
    public static final String SEPARATORINTOLINES = "\n";
    public static final String SEPARATORINTOCELLS = "\",\"";
    public static final int AIRPORTCODECOLUMN = 0;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportIdFile = sc.textFile(AIRPORTIDFILE);
        JavaRDD<String> airportIdLines = airportIdFile.flatMap(
                s -> Arrays.stream(s.split(SEPARATORINTOLINES)).iterator()
        );
        JavaPairRDD<Integer, String> airportCodeDescription = airportIdLines.mapToPair(
                s -> {
                    String[] data = s.split(SEPARATORINTOCELLS);
                    return new Tuple2<>(Integer.parseInt(data[AIRPORTCODECOLUMN].));
                }
        );

    }
}
