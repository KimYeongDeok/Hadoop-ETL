package org.openflamingo.hadoop.mapreduce.replace;

import java.util.HashMap;
import java.util.Map;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GenderMap {
	private Map<String,String> genderMap;

	public GenderMap() {
		this.genderMap = new HashMap<String, String>();
		settingGedndermap();
	}

	private void settingGedndermap() {
		genderMap.put("1","남자");
		genderMap.put("2","여자");
	}

	public String getGenderMap(String numberSex) {
		return genderMap.get(numberSex);
	}
}
