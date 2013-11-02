import java.io.IOException;
import java.io.StringWriter;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxTemperature {

	public static class MaxTempMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split("\\t");
			String maxTempString = words[18];
			String yearString = words[3];
			yearString = yearString.substring(0, 4);

			context.write(new Text(yearString), new Text(maxTempString));

		}
	}

	public static class MaxTempReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			float tempValue = 0;
			float maxTemp = 0;

			for (Text val : values) {

				if (!"\\N".equals(val.toString())) {
					try {
						tempValue = Float.parseFloat(val.toString());
					} catch (Exception e) {
						System.out
								.println(" ==== Error in converting temperature to float ===="
										+ val.toString());
						System.exit(0);
					}

					if (tempValue > maxTemp) {
						maxTemp = tempValue;
					}
				}
			}

			context.write(key, new Text(Float.toString(maxTemp)));
		}
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "word count");
		job.setJarByClass(MaxTemperature.class);
		job.setMapperClass(MaxTempMapper.class);
		job.setCombinerClass(MaxTempReducer.class);
		job.setReducerClass(MaxTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		/*
		 * FileInputFormat.addInputPath(job, new Path(
		 * "/home/nitin/cloudAssignment")); FileOutputFormat.setOutputPath(job,
		 * new Path( "/home/nitin/cloudAssignment/output"));
		 */
		job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		System.out.println("======TOTAL TIME ====== " + (endTime - startTime));
		System.exit(0);
	}
}