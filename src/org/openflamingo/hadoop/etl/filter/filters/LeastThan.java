package org.openflamingo.hadoop.etl.filter.filters;

import org.openflamingo.hadoop.etl.filter.FilterClass;
import org.openflamingo.hadoop.etl.filter.FilterModel;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class LeastThan extends FilterClass {
	@Override
	public boolean doFilter(String coulmn, FilterModel filterModel) {
		int thanValue = Integer.valueOf(filterModel.getTerms());
		int value = Integer.valueOf(coulmn);
		return value < thanValue;
	}
}
