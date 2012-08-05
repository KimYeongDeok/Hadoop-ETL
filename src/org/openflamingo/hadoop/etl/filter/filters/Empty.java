package org.openflamingo.hadoop.etl.filter.filters;

import org.openflamingo.hadoop.etl.filter.FilterClass;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class Empty extends FilterClass{
	public Empty(int columnIndex) {
		super(columnIndex);
	}
	public Empty() {
	}

	@Override
	public boolean doFilter(String coulmn) {
		String trimmedCoulmn = coulmn.trim();
		return terms.equals(trimmedCoulmn);
	}
}
