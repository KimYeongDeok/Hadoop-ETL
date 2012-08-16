package org.openflamingo.hadoop.etl.filter;

/**
 * Filter의 기능을 수행 할 수있다. .
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public interface Filter{
	public boolean service(String[] coulmns) throws InterruptedException;
	public void setFilterModel(FilterModel filterModel);
}
