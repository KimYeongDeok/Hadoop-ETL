package org.openflamingo.hadoop.mapreduce.aggregate;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.mapreduce.ETLDriver;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class AggregateDriver implements ETLDriver {
	@Override
	public int service(Job job,CommandLine cmd, Configuration conf) throws Exception {
	    // Mapper Class
		job.setMapperClass(AggregateMapper.class);

		// Output Key/Value
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);
		return 0;
	}
}