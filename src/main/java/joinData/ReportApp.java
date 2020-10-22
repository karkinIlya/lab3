package joinData;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReportApp {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Report airports");
        job.setJarByClass(ReportApp.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReportAirportsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReportDataMapper.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ReportReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}