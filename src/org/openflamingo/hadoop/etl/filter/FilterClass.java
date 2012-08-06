package org.openflamingo.hadoop.etl.filter;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public abstract class FilterClass implements Filter{
	protected FilterModel filterModel;

//	public FilterClass(String terms, int columnIndex) {
//		this.terms = terms;
//		this.columnIndex = columnIndex;
//	}

	public boolean service(String[] coulmns) throws InterruptedException {
		boolean success = false;

		if(coulmns.length <= filterModel.getColumnIndex() || 0 > filterModel.getColumnIndex())
			throw new InterruptedException("Out of CoulmnIndex");

		if(filterModel.getColumnIndex() > 0)
			success = doFilterInAllCoulmns(coulmns);
		else
			success = doFilterInCoulmn(coulmns, filterModel.getColumnIndex());

		return success;
	}
	private boolean doFilterInCoulmn(String[] columns, int columnIndex) {
		String column = columns[columnIndex];
		return doFilter(column, filterModel);
	}

	private boolean doFilterInAllCoulmns(String[] coulmns){
		boolean isSuccess = true;
		for (String column : coulmns) {
			return doFilter(column, filterModel);
		}
		return false;
	}


	public abstract boolean doFilter(String coulmn, FilterModel filterModel);

	@Override
	public void setFilterModel(FilterModel filterModel) {
		this.filterModel = filterModel;
	}
}
