package org.openflamingo.hadoop.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public interface ETLDriver {
	public int service(Job job, CommandLine cmd, Configuration conf) throws Exception;
}
