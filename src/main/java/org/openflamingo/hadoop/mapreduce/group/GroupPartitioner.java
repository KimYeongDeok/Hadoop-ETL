package org.openflamingo.hadoop.mapreduce.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class GroupPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text Value, int numOfPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numOfPartitions;
    }
}
