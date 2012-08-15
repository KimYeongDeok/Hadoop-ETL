package org.openflamingo.hadoop.etl.filter;

import org.openflamingo.hadoop.etl.ETLObjectHouse;
import org.openflamingo.hadoop.etl.utils.RowUtils;
import org.openflamingo.hadoop.util.StringUtils;

import java.util.ArrayList;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class FilterCriteria {
	private String delimeter = ",";
	private String[] columns;
	private ArrayList<Filter> filters;

	public FilterCriteria(String filterCommand, String delimeter) {
		parseFilterCommand(filterCommand, delimeter);
	}

	private String parseFilterCommand(String filterCommand, String delimeter){
		String[] filterCommands = StringUtils.delimitedListToStringArray(filterCommand, delimeter);
		for (String filter : filterCommands) {
			String[] commands = RowUtils.parseByDelimeter(filter, RowUtils.COMMAND_DELIMETER);

			String commandName = commands[0];
			FilterModel filterModel = createFilterModel(commands);

			FilterClass filterClass = (FilterClass) ETLObjectHouse.findClassByName(commandName);
			filterClass.setFilterModel(filterModel);
			addFilter(filterClass);
		}

		return null;
	}

	private FilterModel createFilterModel(String[] commands){
		int columnIndex = Integer.valueOf(commands[1]);
		String terms = "";
		if(commands.length > 2)
			terms = commands[2];

		FilterModel filterModel = new FilterModel();
		filterModel.setColumnIndex(columnIndex);
		filterModel.setTerms(terms);
		return filterModel;
	}
	public FilterCriteria doFilter(String[] columns) throws InterruptedException {
		this.columns = columns;

		if(filters == null || filters.size() == 0)
			return this;

		for (Filter filter : filters) {
			if(filter.service(this.columns))
				return passFilter();
		}
		this.columns = null;
		return this;
	}

	private FilterCriteria passFilter() {
		return this;
	}

	public void addFilter(Filter filter){
		if(filters == null)
			filters = new ArrayList<Filter>();
		filters.add(filter);
	}

	public String getRow(){
		if(columns == null || columns.length == 0)
			return null;
		StringBuilder stringBuilder = new StringBuilder();
		for (String coulmn : columns) {
			stringBuilder.append(coulmn).append(delimeter);
		}
		stringBuilder.delete(stringBuilder.length()-1, stringBuilder.length());
		return stringBuilder.toString();
	}

	private String[] devideByDelimeter(String row) throws InterruptedException {
		if(row == null)
			throw new InterruptedException("row is null");
		if(row.length() == 0)
			throw new InterruptedException("row contains no data");
		return row.split(delimeter);
	}

	public boolean isRow() {
		return columns != null;
	}
}
