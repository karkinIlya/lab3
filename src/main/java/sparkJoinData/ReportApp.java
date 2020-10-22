package sparkJoinData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class ReportApp {
    public static final String APPNAME = "Report airports";
    public static final String AIRPORTIDFILE = "L_AIRPORT_ID.csv";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportIdFile = sc.textFile(AIRPORTIDFILE);
        JavaRDD<String> splittedAirportId = airportIdFile.flatMap(
                s -> Arrays.stream(s.split(""))
        )
    }
}
