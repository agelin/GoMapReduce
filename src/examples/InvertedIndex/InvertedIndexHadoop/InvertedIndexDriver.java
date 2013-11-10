import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class InvertedIndexDriver {
        public static void main(String[] args) throws Exception {
        	long startTime = System.currentTimeMillis();
                Configuration conf = new Configuration();
                Job job = new Job(conf,"Inverted Index");
                job.setJarByClass(InvertedIndexDriver.class);
                job.setMapperClass(InvertedIndexMapper.class);
                job.setReducerClass(InvertedIndexReducer.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.submit();
                job.waitForCompletion(true);
                long endTime = System.currentTimeMillis();
                FileWriter fw=new FileWriter(args[1]+"/ResponseTime");
                String output = "";
                //double timeSeconds = (endTime - startTime)/1000.0;
                output +="======TOTAL TIME ====== " +(endTime - startTime)  + " Milliseconds";
                fw.write(output);
                fw.close();
        }
}