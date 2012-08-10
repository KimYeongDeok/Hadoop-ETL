package org.openflamingo.hadoop.mapreduce.generate;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.mapreduce.ETLDriver;
import org.openflamingo.hadoop.mapreduce.FrontDriver;
import org.openflamingo.hadoop.util.HdfsUtils;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GenerateDriver implements ETLDriver{
	@Override
	public int service(Job job, CommandLine cmd, Configuration conf) throws Exception {
		CounterGroup counters = countJobMapper(cmd, conf);

		long key = 0;
		for (Counter counter : counters) {
			String simpleName = counter.getName().split("_m_")[1];
			job.getConfiguration().setLong(simpleName, key);
			key += counter.getValue();
		}

		// Mapper Classs
		job.setMapperClass(GenerateMapper.class);

		// Output Key/Value
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.getConfiguration().set("generate", cmd.getOptionValue("generate"));
		job.getConfiguration().set("delimiter", cmd.getOptionValue("delimiter"));

		//Reducer Task
		job.setNumReduceTasks(0);
		return 0;
	}

	private CounterGroup countJobMapper(CommandLine cmd, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
		ProcessCountJob processCountJob = new ProcessCountJob(cmd, conf).invoke();
		if (processCountJob.is())
			throw new InterruptedException("Counter Job runs failed.");

		removeTempOuputFile(cmd);
		return getCounterGroup(processCountJob);
	}

	private void removeTempOuputFile(CommandLine cmd) throws IOException {
		if (HdfsUtils.isExist(cmd.getOptionValue("output"))) {
			HdfsUtils.deleteFromHdfs(cmd.getOptionValue("output"));
		}
	}

	private CounterGroup getCounterGroup(ProcessCountJob processCountJob) throws IOException {
		Job countJob = processCountJob.getCountJob();
		Counters counters =countJob.getCounters();
		return counters.getGroup(GenerateCountMapper.class.getName());
	}

	private void getTotalMapperCount(Counters counters) {
		Counter total = counters.findCounter(GenerateCountMapper.COUNTER.TOTAL_COUNT);
	}


	private class ProcessCountJob {
		private boolean myResult = false;
		private CommandLine cmd;
		private Configuration conf;
		private Job countJob;

		public ProcessCountJob(CommandLine cmd, Configuration conf) {
			this.cmd = cmd;
			this.conf = conf;
		}

		boolean is() {
			return myResult;
		}

		public Job getCountJob() {
			return countJob;
		}

		public ProcessCountJob invoke() throws IOException, InterruptedException, ClassNotFoundException {
			countJob = new Job(conf);
			countJob.setJarByClass(FrontDriver.class);
			FileInputFormat.addInputPaths(countJob, cmd.getOptionValue("input"));
			FileOutputFormat.setOutputPath(countJob, new Path(cmd.getOptionValue("output")));
			countJob.setInputFormatClass(TextInputFormat.class);
			countJob.setOutputFormatClass(TextOutputFormat.class);
			countJob.setMapOutputKeyClass(NullWritable.class);
			countJob.setMapOutputValueClass(Text.class);
			countJob.setMapperClass(GenerateCountMapper.class);
			countJob.setNumReduceTasks(0);
			countJob.waitForCompletion(true);
			boolean success = countJob.waitForCompletion(true);
			if (!success){
				myResult = true;
				return this;
			}
			myResult = false;
			return this;
		}
	}
}
