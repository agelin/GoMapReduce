import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text file = new Text();
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException 
        {
        	//System.out.println(key);
        	//System.out.println(value);
        	//System.out.println("---");
                context.getConfiguration();
                String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
                StringTokenizer st = new StringTokenizer(value.toString());
                while(st.hasMoreTokens())
                {
                        word.set(st.nextToken());
                        file.set(fileName);
                        context.write(word, file);
                }
        }
}