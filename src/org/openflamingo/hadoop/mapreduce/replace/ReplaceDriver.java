package org.openflamingo.hadoop.mapreduce.replace;

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
public class ReplaceDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new ReplaceDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = new Job();

		job.setJarByClass(ReplaceDriver.class);

		// Mapper Class
		job.setMapperClass(ReplaceMapper.class);

		// Output Key/Value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.getConfiguration().set("replace", "0::둘리::메롱,0::마이콜::바보");
		job.getConfiguration().set("delimeter", ",");

		// Reducer Task
//		job.setNumReduceTasks(0);

		// Run a Hadoop Job
		return job.waitForCompletion(true) ? 0 : 1;
	}


}
