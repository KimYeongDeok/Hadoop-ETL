package org.openflamingo.hadoop.etl.filter;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class FilterModel {
	private String terms;
	private int columnIndex = -1;

	public String getTerms() {
		return terms;
	}

	public void setTerms(String terms) {
		this.terms = terms;
	}

	public int getColumnIndex() {
		return columnIndex;
	}

	public void setColumnIndex(int columnIndex) {
		this.columnIndex = columnIndex;
	}
}
