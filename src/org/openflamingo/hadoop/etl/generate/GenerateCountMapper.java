package org.openflamingo.hadoop.etl.generate;

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
public class GenerateCountMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	private String taskID;

	public static enum COUNTER{
		TOTAL_COUNT
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		taskID = String.valueOf(context.getTaskAttemptID().getTaskID().getId());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(this.getClass().getName(), taskID).increment(1);
		context.getCounter(COUNTER.TOTAL_COUNT).increment(1);
	}
}
