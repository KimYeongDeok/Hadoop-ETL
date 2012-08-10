package org.openflamingo.hadoop.mapreduce;

import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.openflamingo.hadoop.etl.ETLHouse;
import org.openflamingo.hadoop.mapreduce.aggregate.AggregateDriver;
import org.openflamingo.hadoop.mapreduce.clean.CleanDriver;
import org.openflamingo.hadoop.mapreduce.clean.CleanMapper;
import org.openflamingo.hadoop.mapreduce.filter.FilterDriver;
import org.openflamingo.hadoop.mapreduce.generate.GenerateDriver;
import org.openflamingo.hadoop.mapreduce.group.GroupDriver;
import org.openflamingo.hadoop.mapreduce.rank.RankDriver;
import org.openflamingo.hadoop.mapreduce.replace.ReplaceDriver;

/**
 * Command Line Parser를 이용한 Hadoop MapReduce Sample Driver.
 */
public class FrontDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new FrontDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(FrontDriver.class);

		CommandLine cmd= null;
		try{
			cmd = parseArguements(args, job);
		}catch(Exception e){
			e.printStackTrace();
			return -1;
		}

		ETLDriver driver = null;
		if (cmd.hasOption("aggregate"))
			driver = new AggregateDriver();
		if (cmd.hasOption("clean"))
			driver = new CleanDriver();
		if (cmd.hasOption("filter"))
			driver = new FilterDriver();
		if (cmd.hasOption("replace"))
			driver = new ReplaceDriver();
		if (cmd.hasOption("group"))
			driver = new GroupDriver();
		if (cmd.hasOption("generate"))
			driver = new GenerateDriver();
		if (cmd.hasOption("rank"))
			driver = new RankDriver();

		assert driver != null;
		driver.service(job, cmd, getConf());

		// Run a Hadoop Job
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private CommandLine parseArguements(String[] args, Job job) throws Exception {
		CommandLine cmd = new CommandInspector().parseCommandLine(args);

		////////////////////////////////////////
		// 파라미터를 Hadoop Configuration에 추가한다
		////////////////////////////////////////

		if (cmd.hasOption("input")) {
			FileInputFormat.addInputPaths(job, cmd.getOptionValue("input"));
		}

		if (cmd.hasOption("output")) {
			FileOutputFormat.setOutputPath(job, new Path(cmd.getOptionValue("output")));
		}

		if (cmd.hasOption("delimiter")) {
			job.getConfiguration().set("delimiter", cmd.getOptionValue("delimiter"));
		}
		return cmd;
	}
}



		// 옵션을 지정한 경우 출력 경로를 삭제한다.
//		if (cmd.hasOption("delete")) {
//			if (HdfsUtils.isExist(cmd.getOptionValue("delete"))) {
//				HdfsUtils.deleteFromHdfs(cmd.getOptionValue("delete"));
//			}
//		}