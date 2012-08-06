package org.openflamingo.hadoop.etl.filter.filters;

import org.openflamingo.hadoop.etl.filter.FilterClass;
import org.openflamingo.hadoop.etl.filter.FilterModel;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class EndWith extends FilterClass {
	@Override
	public boolean doFilter(String coulmn, FilterModel filterModel) {
		String target = filterModel.getTerms();
		int index = coulmn.lastIndexOf(target);
		int lastWordPosition = coulmn.length() - target.length();
		return index == lastWordPosition;
	}
}
