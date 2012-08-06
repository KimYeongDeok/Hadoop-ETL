package org.openflamingo.hadoop.etl.filter;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public interface Filter{
	public boolean service(String[] coulmns) throws InterruptedException;
	public void setFilterModel(FilterModel filterModel);
}
