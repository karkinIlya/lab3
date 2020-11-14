package sparkJoinData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class ReportApp {
    public static final String APP_NAME = "Report airports";
    public static final String AIRPORT_ID_FILE = "L_AIRPORT_ID.csv";
    public static final String SEPARATOR_INTO_CELLS = ",";
    public static final int AIRPORT_CODE_COLUMN = 0;
    public static final int AIRPORT_DESCRIPTION_COLUMN = 1;
    public static final String AIRPORT_DATA_FILE = "664600583_T_ONTIME_sample.csv";
    public static final int ORGIN_AIRPORT_ID_COLUMN = 11;
    public static final int DESTINATION_AIRPORT_ID_COLUMN = 14;
    public static final int NEW_DELAY_COLUMN = 18;
    public static final int CANSELLED_COLUMN = 19;
    public static final String AIRPORT_DATA_TITLE = "\"YEAR\",\"QUARTER\",\"MONTH\",\"DAY_OF_MONTH\",\"DAY_OF_WEEK\"," +
            "\"FL_DATE\",\"UNIQUE_CARRIER\",\"AIRLINE_ID\",\"CARRIER\",\"TAIL_NUM\",\"FL_NUM\",\"ORIGIN_AIRPORT_ID\"," +
            "\"ORIGIN_AIRPORT_SEQ_ID\",\"ORIGIN_CITY_MARKET_ID\",\"DEST_AIRPORT_ID\",\"WHEELS_ON\",\"ARR_TIME\"," +
            "\"ARR_DELAY\",\"ARR_DELAY_NEW\",\"CANCELLED\",\"CANCELLATION_CODE\",\"AIR_TIME\",\"DISTANCE\",";
    public static final String AIRPORT_ID_TITLE = "Code,Description";
    public static final int MAX_DELAY_COLUMN = 0;
    public static final int PART_OF_DELAYS_COLUMN = 1;
    public static final int PART_OF_CANSELLED_COLUMN = 2;
    public static final String OUTPUT_PATH = "output";
    public static final String LOG_FORMAT_STRING = "Max delay: %.1f\t" +
            "Part of delays: %.1f%%\t" +
            "Part of canselled: %.1f%%\t" +
            "Origin airport: %d\t%s\t" +
            "Destination airport: %d\t%s";
    public static final int PERSENT_MULTIPLIER = 100;
    public static final String NO_CANSELLED = "0.00";
    public static final double NO_DELAY = 0.0;

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(APP_NAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaPairRDD<Integer, String> airportInfo = getAirportId(sc);
        final JavaPairRDD<Tuple2<Integer, Integer>, Double[]> airportData = getAirportData(sc);

        final Map<Integer, String> airports = airportInfo.collectAsMap();
        final Broadcast<Map<Integer, String>> airportsBroadcasted = sc.broadcast(airports);
        final JavaRDD<String> reports = airportData
                .map(s -> logFormation(airportsBroadcasted, s));

        reports.saveAsTextFile(OUTPUT_PATH);
    }

    private static String logFormation(Broadcast<Map<Integer, String>>
                                               airportsBroadcasted, Tuple2<Tuple2<Integer, Integer>, Double[]> s) {
        Double[] value = s._2;
        Tuple2<Integer, Integer> key = s._1;
        return String.format(LOG_FORMAT_STRING,
                value[MAX_DELAY_COLUMN],
                value[PART_OF_DELAYS_COLUMN] * PERSENT_MULTIPLIER,
                value[PART_OF_CANSELLED_COLUMN] * PERSENT_MULTIPLIER,
                key._1, airportsBroadcasted.value().get(key._1),
                key._2, airportsBroadcasted.value().get(key._2));
    }

    private static JavaPairRDD<Tuple2<Integer, Integer>, Double[]> getAirportData(JavaSparkContext sc) {
        JavaPairRDD<Tuple2<Integer, Integer>, Double[]> airportData = sc
                .textFile(AIRPORT_DATA_FILE)
                .filter(
                        s -> !s.equals(AIRPORT_DATA_TITLE)
                )
                .mapToPair(
                        s -> {
                            final String[] data = s.split(SEPARATOR_INTO_CELLS);
                            final Tuple2<Integer, Integer> key =
                                    new Tuple2<>(Integer.parseInt(data[ORGIN_AIRPORT_ID_COLUMN]),
                                            Integer.parseInt(data[DESTINATION_AIRPORT_ID_COLUMN]));
                            final AirportStatistic value = new AirportStatistic(
                                    data[NEW_DELAY_COLUMN].isEmpty() ?
                                            NO_DELAY : Double.parseDouble(data[NEW_DELAY_COLUMN]),
                                    !data[CANSELLED_COLUMN].equals(NO_CANSELLED));
                            return new Tuple2<>(key, value);
                        }
                )
                .reduceByKey(
                        (s1, s2) ->  new AirportStatistic(s1, s2)
                )
                .mapToPair(
                        s -> {
                            final Double[] value = {s._2.getMaxDelay(),
                                    (double)s._2.getCountDelay() / s._2.getCount(),
                                    (double)s._2.getCountCanselled() / s._2.getCount()};
                            return new Tuple2<>(s._1, value);
                        }
                );
        return airportData;
    }

    private static String removeQuotes(String str) {
        return str.replace("\"", "");
    }

    private static JavaPairRDD<Integer, String> getAirportId(JavaSparkContext sc) {
        JavaPairRDD<Integer, String> airportInfo = sc
                .textFile(AIRPORT_ID_FILE)
                .filter(
                        s -> !s.equals(AIRPORT_ID_TITLE)
                )
                .mapToPair(
                        s -> {
                            final String[] data = s.split(SEPARATOR_INTO_CELLS);
                            return new Tuple2<>(
                                    Integer.parseInt(removeQuotes(data[AIRPORT_CODE_COLUMN])),
                                    removeQuotes(data[AIRPORT_DESCRIPTION_COLUMN]));
                        }
                );
        return airportInfo;
    }
}
