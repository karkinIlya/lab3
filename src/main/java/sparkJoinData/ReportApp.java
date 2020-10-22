package sparkJoinData;

import org.apache.spark.SparkConf;

public class ReportApp {
    public static final String APP_NAME = "Report airports";
    SparkConf conf = new SparkConf().setAppName(APP_NAME);

}
