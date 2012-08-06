package org.openflamingo.hadoop.mapreduce.grep;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GrepDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new GrepDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = new Job();

		job.setJarByClass(GrepDriver.class);

		// Mapper Class
		job.setMapperClass(GrepMapper.class);

		// Output Key/Value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.getConfiguration().set("grep", "둘리");
		job.getConfiguration().set("delimeter", ",");

		// Reducer Task
//		job.setNumReduceTasks(0);
		// Run a Hadoop Job
		return job.waitForCompletion(true) ? 0 : 1;
	}


}