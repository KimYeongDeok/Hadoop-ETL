package org.openflamingo.hadoop.etl.filter;

import org.openflamingo.hadoop.etl.ETLHouse;
import org.openflamingo.hadoop.etl.utils.Row;
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
	private String[] coulmns;
	private ArrayList<Filter> filters;

	public FilterCriteria() {
	}

	public FilterCriteria(String row) throws InterruptedException {
		this.coulmns = devideByDelimeter(row);
	}
	public FilterCriteria(String row, String delimeter) throws InterruptedException {
		this.delimeter = delimeter;
		this.coulmns = devideByDelimeter(row);
	}

	public String parseFilterCommand(String filterCommand, String delimeter){
		String[] filterCommands = StringUtils.delimitedListToStringArray(filterCommand, delimeter);
		for (String filter : filterCommands) {
			String[] commands = Row.parseByDelimeter(filter, Row.COMMAND_DELIMETER);

			String commandName = commands[0];
			FilterModel filterModel = buildFilterModel(commands);

			addFilter(ETLHouse.buildFilter(commandName, filterModel));
		}

		return null;
	}

	private FilterModel buildFilterModel(String[] commands){
		int columnIndex = Integer.valueOf(commands[1]);
		String terms = "";
		if(commands.length > 2)
			terms = commands[2];

		FilterModel filterModel = new FilterModel();
		filterModel.setColumnIndex(columnIndex);
		filterModel.setTerms(terms);
		return filterModel;
	}
	public FilterCriteria doFilter(String row) throws InterruptedException {
		setCoulmns(row);

		if(filters == null || filters.size() == 0)
			return this;

		for (Filter filter : filters) {
			if(filter.service(coulmns))
				return this;
		}
		coulmns = null;
		return this;
	}
	public void addFilter(Filter filter){
		if(filters == null)
			filters = new ArrayList<Filter>();
		filters.add(filter);
	}

	public String getRow(){
		if(coulmns == null || coulmns.length == 0)
			return null;
		StringBuilder stringBuilder = new StringBuilder();
		for (String coulmn : coulmns) {
			stringBuilder.append(coulmn).append(delimeter);
		}
		stringBuilder.delete(stringBuilder.length()-1, stringBuilder.length());
		return stringBuilder.toString();
	}

	public void setCoulmns(String row) throws InterruptedException {
		this.coulmns = devideByDelimeter(row);
	}

	public boolean isRow(){
		return !(coulmns == null || coulmns.length == 0);
	}

	private String[] devideByDelimeter(String row) throws InterruptedException {
		if(row == null)
			throw new InterruptedException("row is null");
		if(row.length() == 0)
			throw new InterruptedException("row contains no data");
		return row.split(delimeter);
	}

}
