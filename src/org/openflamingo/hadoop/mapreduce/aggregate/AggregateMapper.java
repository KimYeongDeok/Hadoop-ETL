package org.openflamingo.hadoop.mapreduce.aggregate;

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
public class AggregateMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private static final String DEMITER = ",";
	public static final int COLUMN_SEX = 2;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), value);
	}
}
