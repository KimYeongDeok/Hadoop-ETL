import org.apache.commons.cli.CommandLine;
import org.junit.Test;
import org.openflamingo.hadoop.mapreduce.CommandInspector;

import java.util.Arrays;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ParserTest {
	@Test
	public void adsfs() throws Exception {
		String[] args = {"-input","asdf","sdf","-output","eee","-delimiter",","};

		CommandLine cmd = new CommandInspector().parseCommandLine(args);

		System.out.println(Arrays.toString(cmd.getOptionValues("input")));
	}
}
