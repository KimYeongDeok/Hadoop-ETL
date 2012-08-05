package org.openflamingo.hadoop.etl.filter.filters;

import org.openflamingo.hadoop.etl.filter.FilterClass;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class Equals extends FilterClass {
	public Equals(String terms, int columnIndex) {
		super(terms, columnIndex);
	}
	public Equals(String terms) {
		super(terms);
	}

	@Override
	public boolean doFilter(String coulmn) {
		return terms.equals(coulmn);
	}

}
