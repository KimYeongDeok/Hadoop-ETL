package org.openflamingo.hadoop.mapreduce.clean;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CleanBuild {
	public static Clean build(String command, String delimeter) {
		int[] commands = parseCleanCommand(command);
		Clean clean = new Clean(commands, delimeter);
		return clean;
	}

	private static int[] parseCleanCommand(String command) {
		String[] parameterCommands = command.split(",");
		return toIntArray(parameterCommands);
	}

	public static int[] toIntArray(String[] strings){
		int[] ints = new int[strings.length];
		for (int i = 0; i < ints.length; i++) {
			ints[i] = Integer.parseInt(strings[i]);
		}
		return ints;
	}

}
