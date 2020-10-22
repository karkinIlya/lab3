package sparkJoinData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class ReportApp {
    public static final String APPNAME = "Report airports";

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName(APPNAME);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> 
    }
}
