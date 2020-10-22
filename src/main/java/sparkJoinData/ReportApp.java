package sparkJoinData;

import org.apache.spark.SparkConf;

public class ReportApp {
    public static final String APPNAME = "Report airports";
    SparkConf conf = new SparkConf().setAppName(APPNAME);

}
