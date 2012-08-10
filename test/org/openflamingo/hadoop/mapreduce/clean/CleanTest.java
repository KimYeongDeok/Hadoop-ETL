package org.openflamingo.hadoop.mapreduce.clean;

import org.junit.Assert;
import org.junit.Test;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class CleanTest {

	@Test
	public void testDoClean() throws Exception {
		Clean clean = new Clean(new int[]{0, 1}, ",");
		String result = clean.doClean("1,2,3,4,5");
		Assert.assertEquals("3,4,5", result);
	}

	@Test
	public void testDoClean2() throws Exception {
		Clean clean = new Clean(CleanBuild.parseCleanCommand("0,1"), ",");
		String result = clean.doClean("1,2,3,4,5");
		Assert.assertEquals("3,4,5", result);
	}
}