package org.openflamingo.hadoop.etl.filter;

/**
 * Filter의 기본적인 기능을 수행한다. Filter의 coulmn위치 값이 입력되지 않을 경우에는
 * 모든 column에 대하여 기능을 수행하고 coulmn위치 값이 있을 경우 한 coulmn만 기능을
 * 수행한다.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public abstract class FilterClass implements Filter{
	/** filterModel은 각 Filter에 전달될 정보*/
	protected FilterModel filterModel;

	/**
	 *
	 * @param coulmns 
	 * @return
	 * @throws InterruptedException
	 */
	public boolean service(String[] coulmns) throws InterruptedException {
		boolean success = false;

		if(coulmns.length <= filterModel.getColumnIndex())
			throw new InterruptedException("Out of CoulmnIndex" + coulmns.length);

		if(filterModel.getColumnIndex() < 0)
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
