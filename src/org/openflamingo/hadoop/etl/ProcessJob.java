package org.openflamingo.hadoop.etl;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.openflamingo.hadoop.mapreduce.FrontDriver;
import org.openflamingo.hadoop.util.HdfsUtils;

import java.io.IOException;

public class ProcessJob {
	private boolean myResult = false;
	private CommandLine cmd;
	private Configuration conf;
	private Job job;
	private Class maapperClass;

	public ProcessJob(CommandLine cmd, Configuration conf) {
		this.cmd = cmd;
		this.conf = conf;
	}

	public boolean is() {
		return myResult;
	}

	public Job getJob() {
		return job;
	}

	public ProcessJob invoke(Class maapperClass) throws IOException, InterruptedException, ClassNotFoundException {
		this.maapperClass = maapperClass;
		job = new Job(conf);
		job.setJarByClass(FrontDriver.class);
		FileInputFormat.addInputPaths(job, cmd.getOptionValue("input"));
		FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("output")));
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(maapperClass);
		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
		boolean success = job.waitForCompletion(true);
		if (!success) {
			myResult = true;
			return null;
		}
		myResult = false;
		removeTempOuputFile(cmd);
		return null;
	}
	private void removeTempOuputFile(CommandLine cmd) throws IOException {
		if (HdfsUtils.isExist(cmd.getOptionValue("output"))) {
			HdfsUtils.deleteFromHdfs(cmd.getOptionValue("output"));
		}
	}
	public CounterGroup getCounterGroup() throws IOException {
		Counters counters =job.getCounters();
		return counters.getGroup(maapperClass.getClass().getName());
	}
}