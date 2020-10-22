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
    public static final String SEPARATORINTOCELLS = ",";
    public static final int AIRPORTCODECOLUMN = 0;
    public static final String QUOTION = "\"";
    public static final String EMPTY = "";
    public static final int AIRPORTDESCRIPTIONCOLUMN = 1;
    public static final String AIRPORTDATAFILE = "664600583_T_ONTIME_sample.csv";
    public static final int ORGINAIRPORTIDCOLUMN = 11;
    public static final int DESTINATIONAIRPORTIDCOLUMN = 14;
    public static final int NEWDELAYCOLUMN = 18;
    public static final int CANSELLEDCOLUMN = 19;
    public static final int CANSELLEDCOLUMNINGROUPBYKEY = 1;
    public static final int DELAYCOLUMNINGROUPBYKEY = 0;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer, String[]> airportInfo = sc
                .textFile(AIRPORTIDFILE)
                .mapToPair(
                        s -> {
                            final String[] data = s.split(SEPARATORINTOCELLS);
                            return new Tuple2<>(Integer.parseInt(data[AIRPORTCODECOLUMN].replace(QUOTION, EMPTY)),
                                    new String[] {data[AIRPORTDESCRIPTIONCOLUMN].replace(QUOTION, EMPTY)});
                        }
                );

        JavaPairRDD<Tuple2<Integer, Integer>, Double[]> airportData = sc
                .textFile(AIRPORTDATAFILE)
                .filter(
                        s -> !s.equals("\"YEAR\",\"QUARTER\",\"MONTH\",\"DAY_OF_MONTH\",\"DAY_OF_WEEK\",\"FL_DATE\",\"UNIQUE_CARRIER\",\"AIRLINE_ID\",\"CARRIER\",\"TAIL_NUM\",\"FL_NUM\",\"ORIGIN_AIRPORT_ID\",\"ORIGIN_AIRPORT_SEQ_ID\",\"ORIGIN_CITY_MARKET_ID\",\"DEST_AIRPORT_ID\",\"WHEELS_ON\",\"ARR_TIME\",\"ARR_DELAY\",\"ARR_DELAY_NEW\",\"CANCELLED\",\"CANCELLATION_CODE\",\"AIR_TIME\",\"DISTANCE\",")
                )
                .mapToPair(
                        s -> {
                            final String[] data = s.split(SEPARATORINTOCELLS);
                            final Tuple2<Integer, Integer> key =
                                    new Tuple2<>(Integer.parseInt(data[ORGINAIRPORTIDCOLUMN]),
                                            Integer.parseInt(data[DESTINATIONAIRPORTIDCOLUMN]));
                            final String[] value = {data[NEWDELAYCOLUMN], data[CANSELLEDCOLUMN]};
                            return new Tuple2<>(key, value);
                        }
                )
                .groupByKey()
                .mapToPair(
                        s -> {
                            double maxDelay = 0;
                            int delayCount = 0, cancelledCount = 0, count = 0;
                            for (String[] str : s._2) {
                                count++;
                                if(!str[CANSELLEDCOLUMNINGROUPBYKEY].equals("0.00")) {
                                    cancelledCount++;
                                }
                                else if (!str[DELAYCOLUMNINGROUPBYKEY].equals("0.00")
                                        && !str[DELAYCOLUMNINGROUPBYKEY].isEmpty()) {
                                    delayCount++;
                                    double curDelay = Double.parseDouble(str[DELAYCOLUMNINGROUPBYKEY]);
                                    maxDelay = maxDelay >= curDelay ? maxDelay : curDelay;
                                }
                            }
                            final Double[] value = {maxDelay, (double)delayCount / count,
                                    (double)cancelledCount / count};
                            return new Tuple2<>(s._1, value);
                        }
                );

        System.out.println(sc
                .textFile(AIRPORTDATAFILE)
                .filter(
                        s -> s.charAt(0) != '\"'
                )
                .mapToPair(
                        s -> {
                            final String[] data = s.split(SEPARATORINTOCELLS);
                            final Tuple2<Integer, Integer> key =
                                    new Tuple2<>(Integer.parseInt(data[ORGINAIRPORTIDCOLUMN]),
                                            Integer.parseInt(data[DESTINATIONAIRPORTIDCOLUMN]));
                            final String[] value = {data[NEWDELAYCOLUMN], data[CANSELLEDCOLUMN]};
                            return new Tuple2<>(key, value);
                        }
                )
                .groupByKey()
                .map(
                        s -> {
                            double maxDelay = 0;
                            int delayCount = 0, cancelledCount = 0, count = 0;
                            for (String[] str : s._2) {
                                count++;
                                if(!str[CANSELLEDCOLUMNINGROUPBYKEY].equals("0.00")) {
                                    cancelledCount++;
                                }
                                else if (!str[DELAYCOLUMNINGROUPBYKEY].equals("0.00")
                                        && !str[DELAYCOLUMNINGROUPBYKEY].isEmpty()) {
                                    delayCount++;
                                    double curDelay = Double.parseDouble(str[DELAYCOLUMNINGROUPBYKEY]);
                                    maxDelay = maxDelay >= curDelay ? maxDelay : curDelay;
                                }
                            }
                            return s._1._1.toString() + " " + s._1._2.toString() + count + " " + maxDelay;
                        }
                )
                .collect());
    }
}
