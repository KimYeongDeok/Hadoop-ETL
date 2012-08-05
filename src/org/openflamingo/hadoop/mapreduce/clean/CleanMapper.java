package org.openflamingo.hadoop.mapreduce.clean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private static final String DEMITER = ",";
	public static final int COLUMN_SEX = 2;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(",");

		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < values.length; i++) {
			if(i == COLUMN_SEX)
				continue;
			String text = values[i];
			buffer.append(text).append(DEMITER);
		}
		buffer.delete(buffer.length()-1, buffer.length());

		context.write(NullWritable.get(), new Text(buffer.toString()));
	}
}
