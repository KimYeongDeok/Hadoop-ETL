import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.openflamingo.hadoop.util.HdfsUtils;
import org.junit.Test;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ParseTest {
	private Options getOptions() {
		Options options = new Options();
		options.addOption("input", "input", true, "입력 경로 (필수)");
		options.addOption("output", "output", true, "출력 경로 (필수)");
		options.addOption("delimiter", "delimiter", true, "컬럼 구분자 (필수)");
		options.addOption("clean", "clean", true, "입력값 (필수)");
		options.addOption("delete", "delete", true, "출력 경로가 이미 존재하는 경우 삭제");
		return options;
	}
	@Test
	public void test() throws ParseException {
		String[] args = {"-input", "youngdeok/etl_data/", "-output", "youngdeok/etl_clean/output", "-delete", "1", "-clean", "0,0", "-delimiter", ","};
		Options options = getOptions();
		HelpFormatter formatter = new HelpFormatter();

		// 커맨드 라인을 파싱한다.
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);



		////////////////////////////////////////
		// 파라미터를 Hadoop Configuration에 추가한다
		////////////////////////////////////////
		System.out.println(cmd.getArgs().toString());
		System.out.println(cmd.getOptionValue("input"));
		System.out.println(cmd.getOptionValue("output"));
		System.out.println(cmd.getOptionValue("clean"));
		System.out.println(cmd.getOptionValue("delimiter"));
		System.out.println(cmd.getOptionValue("delete"));
		if (cmd.hasOption("i")) {

		}

		if (cmd.hasOption("o")) {

		}

		if (cmd.hasOption("d")) {

		}

		// 옵션을 지정한 경우 출력 경로를 삭제한다.
		if (cmd.hasOption("od")) {

		}

	}
}
