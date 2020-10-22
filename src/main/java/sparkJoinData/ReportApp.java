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
    public static final String QUOTION = "\"";
    public static final String EMPTY = "";
    public static final int AIRPORTDESCRIPTIONCOLUMN = 1;
    public static final String AIRPORTDATAFILE = "664600583_T_ONTIME_sample.csv";
    public static final int ORGINAIRPORTIDCOLUMN = 11;
    public static final int DESTINATIONAIRPORTIDCOLUMN = 14;
    public static final int NEWDELAYCOLUMN = 18;
    public static final int CANSELLEDCOLUMN = 19;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, String[]> airportInfo = sc
                .textFile(AIRPORTIDFILE)
                .flatMap(
                        s -> Arrays.stream(s.split(SEPARATORINTOLINES)).iterator()
                )
                .mapToPair(
                        s -> {
                            String[] data = s.split(SEPARATORINTOCELLS);
                            return new Tuple2<>(Integer.parseInt(data[AIRPORTCODECOLUMN].replace(QUOTION, EMPTY)),
                                    new String[] {data[AIRPORTDESCRIPTIONCOLUMN].replace(QUOTION, EMPTY)});
                        }
                );

        JavaPairRDD<Tuple2<Integer, Integer>, Double[]> airportData = sc
                .textFile(AIRPORTDATAFILE)
                .flatMap(
                        s -> Arrays.stream(s.split(SEPARATORINTOCELLS)).iterator()
                )
                .mapToPair(
                        s -> {
                            String[] data = s.split(SEPARATORINTOCELLS);
                            Tuple2<Integer, Integer> key = new Tuple2<>(Integer.parseInt(data[ORGINAIRPORTIDCOLUMN]),
                                    Integer.parseInt(data[DESTINATIONAIRPORTIDCOLUMN]));
                            Double[] value = {Double.parseDouble(data[NEWDELAYCOLUMN]),
                                    Double.parseDouble(data[CANSELLEDCOLUMN])};
                            return new Tuple2<>(key, value);
                        }
                )
                .groupByKey()
                .map(
                        s -> {
                            double maxDelay = 0, delaySum = 0;
                        }
                )
    }
}
