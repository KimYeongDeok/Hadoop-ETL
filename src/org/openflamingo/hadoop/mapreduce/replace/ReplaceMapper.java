package org.openflamingo.hadoop.mapreduce.replace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openflamingo.hadoop.etl.filter.FilterCriteria;
import org.openflamingo.hadoop.etl.filter.filters.Equals;
import org.openflamingo.hadoop.etl.filter.filters.NotEmpty;
import org.openflamingo.hadoop.etl.replace.ReplaceCriteria;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ReplaceMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		ReplaceCriteria replaceCriteria = new ReplaceCriteria();


		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



		context.write(NullWritable.get(), new Text(""));
	}
}
