package org.openflamingo.hadoop.mapreduce.filter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openflamingo.hadoop.etl.filter.FilterCriteria;
import org.openflamingo.hadoop.etl.filter.filters.Equals;
import org.openflamingo.hadoop.etl.filter.filters.NotEmpty;

import java.io.IOException;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	private FilterCriteria criteria;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		criteria = new FilterCriteria();
		criteria.addFilter(new Equals("둘리", 0));
		criteria.addFilter(new NotEmpty(0));

		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		criteria.setCoulmns(value.toString());
		criteria.doFilter();

		if(criteria.isRow())
			context.write(NullWritable.get(), new Text(criteria.getRow()));
	}
}
