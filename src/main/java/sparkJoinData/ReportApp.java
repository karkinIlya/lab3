package sparkJoinData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

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
    public static final String AIRPORTDATATITLE = "\"YEAR\",\"QUARTER\",\"MONTH\",\"DAY_OF_MONTH\",\"DAY_OF_WEEK\",\"FL_DATE\",\"UNIQUE_CARRIER\",\"AIRLINE_ID\",\"CARRIER\",\"TAIL_NUM\",\"FL_NUM\",\"ORIGIN_AIRPORT_ID\",\"ORIGIN_AIRPORT_SEQ_ID\",\"ORIGIN_CITY_MARKET_ID\",\"DEST_AIRPORT_ID\",\"WHEELS_ON\",\"ARR_TIME\",\"ARR_DELAY\",\"ARR_DELAY_NEW\",\"CANCELLED\",\"CANCELLATION_CODE\",\"AIR_TIME\",\"DISTANCE\",";
    public static final String AIRPORTIDTITLE = "Code,Description";
    public static final int MAXDELAYCOLUMN = 0;
    public static final int PARTOFDELAYSCOLUMN = 1;
    public static final int PARTOFCANSELLEDCOLUMN = 2;
    public static final String OUTPUTPATH = "output";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaPairRDD<Integer, String> airportInfo = getAirportId(sc);
        final JavaPairRDD<Tuple2<Integer, Integer>, Double[]> airportData = getAirportData(sc);

        final Map<Integer, String> airports = airportInfo.collectAsMap();
        final Broadcast<Map<Integer, String>> airportsBroadcasted = sc.broadcast(airports);
        final JavaRDD<String> reports = airportData
                .map(s -> logFormation(airportsBroadcasted, s));

        reports.saveAsTextFile(OUTPUTPATH);
    }

    private static String logFormation(Broadcast<Map<Integer, String>> airportsBroadcasted, Tuple2<Tuple2<Integer, Integer>, Double[]> s) {
        return "Max delay: " + String.format("%.3f", s._2[MAXDELAYCOLUMN]) + "\t" +
                "Part of delays: " + String.format("%.3f", s._2[PARTOFDELAYSCOLUMN] * 100) + "%\t" +
                "Part of canselled: " + String.format("%.3f", s._2[PARTOFCANSELLEDCOLUMN] * 100) + "%\t" +
                "Origin airport: " + s._1._1 + "\t" + airportsBroadcasted.value().get(s._1._1) + "\t" +
                "Destination airport: " + s._1._2 + "\t" + airportsBroadcasted.value().get(s._1._2);
    }

    private static JavaPairRDD<Tuple2<Integer, Integer>, Double[]> getAirportData(JavaSparkContext sc) {
        JavaPairRDD<Tuple2<Integer, Integer>, Double[]> airportData = sc
                .textFile(AIRPORTDATAFILE)
                .filter(
                        s -> !s.equals(AIRPORTDATATITLE)
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
        return airportData;
    }

    private static JavaPairRDD<Integer, String> getAirportId(JavaSparkContext sc) {
        JavaPairRDD<Integer, String> airportInfo = sc
                .textFile(AIRPORTIDFILE)
                .filter(
                        s -> !s.equals(AIRPORTIDTITLE)
                )
                .mapToPair(
                        s -> {
                            final String[] data = s.split(SEPARATORINTOCELLS);
                            return new Tuple2<>(Integer.parseInt(data[AIRPORTCODECOLUMN].replace(QUOTION, EMPTY)),
                                    data[AIRPORTDESCRIPTIONCOLUMN].replace(QUOTION, EMPTY));
                        }
                );
        return airportInfo;
    }
}
