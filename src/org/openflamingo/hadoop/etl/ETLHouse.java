package org.openflamingo.hadoop.etl;

import org.openflamingo.hadoop.etl.filter.Filter;
import org.openflamingo.hadoop.etl.filter.FilterModel;
import org.openflamingo.hadoop.etl.filter.filters.Equals;

import java.util.HashMap;
import java.util.Map;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class ETLHouse {
	private static Map<String, String> filterMap =new HashMap<String, String>();
	static {
		filterMap.put("aggregate", "org.openflamingo.hadoop.mapreduce.aggregate.AggregateDriver");
		filterMap.put("clean", "org.openflamingo.hadoop.mapreduce.clean.CleanDriver");
		filterMap.put("filter", "org.openflamingo.hadoop.mapreduce.filter.FilterDriver");
		filterMap.put("grep", "org.openflamingo.hadoop.mapreduce.grep.GrepDriver");
		filterMap.put("replace", "org.openflamingo.hadoop.mapreduce.replace.ReplaceDriver");

		filterMap.put("EQ", "org.openflamingo.hadoop.etl.filter.filters.Equals");
		filterMap.put("NEQ", "org.openflamingo.hadoop.etl.filter.filters.NotEquals");
		filterMap.put("EMPTY", "org.openflamingo.hadoop.etl.filter.filters.Empty");
		filterMap.put("NEMPTY", "org.openflamingo.hadoop.etl.filter.filters.NotEmpty");
		filterMap.put("STARTWITH", "org.openflamingo.hadoop.etl.filter.filters.StartWith");
		filterMap.put("ENDWITH", "org.openflamingo.hadoop.etl.filter.filters.EndWith");
		filterMap.put("GT", "org.openflamingo.hadoop.etl.filter.filters.GreaterThan");
		filterMap.put("GTE", "org.openflamingo.hadoop.etl.filter.filters.GreatThanEquals");
		filterMap.put("LT", "org.openflamingo.hadoop.etl.filter.filters.LeastThan");
		filterMap.put("LTE", "org.openflamingo.hadoop.etl.filter.filters.LeastThanEquals");
	}

	public static String findClassNameByName(String name){
		if(filterMap.containsKey(name))
			return filterMap.get(name);
		return null;
	}
	public static Filter buildFilter(String filterName, FilterModel filterModel) {
		Filter filter = null;
		try {
			filter = (Filter) Class.forName(findClassNameByName(filterName)).newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}

		assert filter != null;
		filter.setFilterModel(filterModel);
		return filter;
	}
}
