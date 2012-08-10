package org.openflamingo.hadoop.mapreduce;

import org.apache.commons.cli.*;

import java.util.Arrays;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CommandInspector {
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
	private Options getOptions() {
		Options options = new Options();
		options.addOption("input", true, "입력 경로 (필수)");
		options.addOption("output", true, "출력 경로 (필수)");
		options.addOption("delimiter", true, "컬럼 구분자 (필수)");
		options.addOption("clean", true, "입력값 (필수)");
		options.addOption("group", true, "입력값 (필수)");
		options.addOption("filter", true, "입력값 (필수)");
		options.addOption("rank", true, "입력값 (필수)");
		options.addOption("aggregate", false, "입력값 없음");
		options.addOption("generate", true, "입력값 없음");
		options.addOption("delete", true, "출력 경로가 이미 존재하는 경우 삭제");
		return options;
	}

	public CommandLine parseCommandLine(String[] args) throws Exception {
		////////////////////////////////////////
		// 옵션 목록을 구성하고 검증한다.
		////////////////////////////////////////
		Options options = getOptions();
		HelpFormatter formatter = new HelpFormatter();
		if (args.length == 0) {
			formatter.printHelp("hadoop jar <JAR> " + getClass().getName(), options, true);
			throw new Exception();
		}

		// 커맨드 라인을 파싱한다.
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);

		// 파라미터를 검증한다.
		for (String[] requiredOption : requiredOptions) {
			if (!cmd.hasOption(requiredOption[0])) {
				formatter.printHelp("hadoop jar <JAR> " + getClass().getName(), options, true);
				throw new Exception();
			}
		}
		return cmd;
	}
}
