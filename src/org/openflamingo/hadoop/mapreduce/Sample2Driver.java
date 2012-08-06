package org.openflamingo.hadoop.mapreduce;

import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.openflamingo.hadoop.mapreduce.clean.CleanMapper;
import org.openflamingo.hadoop.util.HdfsUtils;

import java.util.Arrays;

/**
 * Command Line Parser를 이용한 Hadoop MapReduce Sample Driver.
 */
public class Sample2Driver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {

	/**
	 * 필수 옵션
	 */
	private final String[][] requiredOptions =
		{
			{"input", "입력 경로를 지정해 주십시오. 입력 경로가 존재하지 않으면 MapReduce가 동작할 수 없습니다."},
			{"output", "출력 경로를 지정해 주십시오."},
			{"delimiter", "컬럼의 구분자를 지정해주십시오. CSV 파일의 컬럼을 처리할 수 없습니다."},
		};

	/**
	 * 사용가능한 옵션 목록을 구성한다.
	 *
	 * @return 옵션 목록
	 */
	private static Options getOptions() {
		Options options = new Options();
		options.addOption("input", "input", true, "입력 경로 (필수)");
		options.addOption("output", "output", true, "출력 경로 (필수)");
		options.addOption("delimiter", "delimiter", true, "컬럼 구분자 (필수)");
		options.addOption("clean", "clean", false, "입력값 (필수)");
		options.addOption("delete", "delete", false, "출력 경로가 이미 존재하는 경우 삭제");
		//		options.addOption("filter", "filter", false,  "입력값 (필수)");
		//		options.addOption("replace", "replace", false,  "입력값 (필수)");
		return options;
	}



	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Sample2Driver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Job job = new Job();

		int result = parseArguements(args, job);
		if (result != 0) {
			return result;
		}

		job.setJarByClass(Sample2Driver.class);

		// Mapper Class
		job.setMapperClass(SampleMapper.class);

		// Output Key/Value
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		// Reducer Task
		job.setNumReduceTasks(0);

		// Run a Hadoop Job
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private int parseArguements(String[] args, Job job) throws Exception {
		////////////////////////////////////////
		// 옵션 목록을 구성하고 검증한다.
		////////////////////////////////////////

		Options options = getOptions();
		HelpFormatter formatter = new HelpFormatter();
		if (args.length == 0) {
			formatter.printHelp("hadoop jar <JAR> " + getClass().getName(), options, true);
			return -1;
		}

		// 커맨드 라인을 파싱한다.
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);


		// 파라미터를 검증한다.
		for (String[] requiredOption : requiredOptions) {
			if (!cmd.hasOption(requiredOption[0])) {
				formatter.printHelp("hadoop jar <JAR> " + getClass().getName(), options, true);
				return -1;
			}
		}

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

		// 옵션을 지정한 경우 출력 경로를 삭제한다.
//		if (cmd.hasOption("delete")) {
//			if (HdfsUtils.isExist(cmd.getOptionValue("delete"))) {
//				HdfsUtils.deleteFromHdfs(cmd.getOptionValue("delete"));
//			}
//		}

		if (cmd.hasOption("clean")) {
			job.setMapperClass(CleanMapper.class);
			job.getConfiguration().set("clean", cmd.getOptionValue("clean"));
		}

		return 0;
	}
}