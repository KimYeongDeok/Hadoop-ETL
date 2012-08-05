package org.openflamingo.hadoop.etl.filter;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public abstract class FilterClass implements Filter{
	protected String terms;
	protected int columnIndex = -1;

	public FilterClass(int columnIndex) {
		this.columnIndex = columnIndex;
		this.terms = "";
	}

	public FilterClass(String terms, int columnIndex) {
		this.terms = terms;
		this.columnIndex = columnIndex;
	}

	public FilterClass(String terms) {
		this.terms = terms;
	}

	protected FilterClass() {
	}


	public String[] service(String[] coulmns) throws InterruptedException {
		String[] result = null;
		if(coulmns.length <= columnIndex || 0 > columnIndex)
			throw new InterruptedException("Out of CoulmnIndex");

		if(columnIndex > 0)
			result = doFilterInAllCoulmns(coulmns);
		else
			result = doFilterInCoulmn(coulmns, columnIndex);

		return result;
	}
	private String[] doFilterInCoulmn(String[] coulmns, int columnIndex) {
		String coulumn = coulmns[columnIndex];

		boolean isSuccess = doFilter(coulumn);
		if(isSuccess)
			return coulmns;
		return null;
	}

	private String[] doFilterInAllCoulmns(String[] coulmns){
		boolean isSuccess = true;
		for (String coulmn : coulmns) {
			isSuccess = doFilter(coulmn);
			if(!isSuccess)
				return null;
		}
		return coulmns;
	}


	public abstract boolean doFilter(String coulmn);

}
