import org.eclipse.jdt.internal.core.SourceType;
import org.junit.Test;
import org.openflamingo.hadoop.util.StringUtils;

import java.util.Arrays;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class asdfasdf {
	String delimeter =",";
	int[] columnIndexs = {0,1};
	@Test
	public void asdf(){
//		String s = "-input, youngdeok/etl_data/, -output, youngdeok/etl_clean/output, -delete, true, -delimiter, ,, -clean, 0,0";
		String target = "as";
		String a = "eras";
		int index = a.lastIndexOf(target);
		System.out.println(index);
		System.out.println(a.length()- target.length());
	}

	private int[] parserParameter(String command) {
			String[] parameterCommands = command.split(",");
			return toIntArray(parameterCommands);
		}

		public int[] toIntArray(String[] strings){
			int[] ints = new int[strings.length];
			for (int i = 0; i < ints.length; i++) {
				ints[i] = Integer.parseInt(strings[i]);
			}
			return ints;
		}

	public String doClean(String row){
		String[] columns = StringUtils.delimitedListToStringArray(row, delimeter);

		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < columns.length; i++) {
			if(checkCleanColumn(i))
				continue;
			builder.append(columns[i]).append(delimeter);
		}

		builder.delete(builder.length()-1, builder.length());
		return builder.toString();
	}

	private boolean checkCleanColumn(int index) {
		boolean result = false;
		for (int columnIndex : columnIndexs) {
			if(index == columnIndex)
				return true;
		}
		return result;
	}

}
