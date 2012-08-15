package org.openflamingo.hadoop.mapreduce.rank;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.etl.ProcessJob;
import org.openflamingo.hadoop.etl.generate.GenerateCountMapper;
import org.openflamingo.hadoop.mapreduce.ETLDriver;

import java.io.IOException;


/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class RankDriver implements ETLDriver{

	@Override
	public int service(Job job, CommandLine cmd, Configuration conf) throws Exception {
		CounterGroup counters = countJobMapper(cmd, conf);
		settingCounterToMapper(job, counters);

		// Mapper Classs
		job.setMapperClass(RankMapper.class);
		job.setReducerClass(RankReducer.class);
		// Output Key/Value
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		//		job.getConfiguration().set("rank", cmd.getOptionValue("rank"));
		job.getConfiguration().set("delimiter", cmd.getOptionValue("delimiter"));

		//Reducer Task
		job.setNumReduceTasks(6);
		return 0;
	}
	private CounterGroup countJobMapper(CommandLine cmd, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		ProcessJob processJob = new ProcessJob(cmd, conf).invoke(GenerateCountMapper.class);
		if (!processJob.is())
			throw new InterruptedException("Counter Job runs failed.");
		return processJob.getCounterGroup();
	}

	private void settingCounterToMapper(Job job, CounterGroup counters) {
		long key = 0;
		for (Counter counter : counters) {
			job.getConfiguration().setLong(counter.getName(), key);
			key += counter.getValue();
		}

	}
}