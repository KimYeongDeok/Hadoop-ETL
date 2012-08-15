package org.openflamingo.hadoop.etl.replace;

import org.openflamingo.hadoop.etl.utils.RowUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ReplaceCriteria {
	private List<Replace> replaces;

	public void parseReplaceCommand(String replaceCommand, String delimiter) throws InterruptedException {
		String[] columns = RowUtils.parseByDelimeter(replaceCommand, delimiter);
		for (String column : columns) {
			String[] command = RowUtils.parseByDelimeter(column, RowUtils.COMMAND_DELIMETER);
			addReplace(new Replace(command));
		}
	}
	public void addReplace(Replace replace){
		if(replaces == null)
			replaces = new ArrayList<Replace>();
		replaces.add(replace);
	}

	public void doReplace(String[] columns){
		for (Replace replace : replaces) {
			replace.doReplace(columns);
		}
	}
}
