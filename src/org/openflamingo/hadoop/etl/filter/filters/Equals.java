package org.openflamingo.hadoop.etl.filter.filters;

import org.openflamingo.hadoop.etl.filter.FilterClass;
import org.openflamingo.hadoop.etl.filter.FilterModel;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class Equals extends FilterClass {
	@Override
	public boolean doFilter(String coulmn, FilterModel filterModel) {
		return filterModel.getTerms().equals(coulmn);
	}

}
