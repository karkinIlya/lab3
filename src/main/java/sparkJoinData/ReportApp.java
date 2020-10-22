package sparkJoinData;

import org.apache.spark.SparkConf;

public class ReportApp {
    public static final String APPNAME = "Report airports";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(APPNAME);
    }
}
