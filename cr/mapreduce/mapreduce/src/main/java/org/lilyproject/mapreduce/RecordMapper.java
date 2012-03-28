package org.lilyproject.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * Extends the Mapper interface to fix the input key and input value
 * to correspond to what LilyInputFormat gives you.
 */
public class RecordMapper<KEYOUT, VALUEOUT> extends Mapper<RecordIdWritable, RecordWritable, KEYOUT, VALUEOUT> {
}
