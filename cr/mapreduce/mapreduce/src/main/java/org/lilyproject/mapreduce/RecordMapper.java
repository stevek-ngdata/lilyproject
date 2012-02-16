package org.lilyproject.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;

public class RecordMapper<KEYOUT, VALUEOUT> extends Mapper<RecordIdWritable, RecordWritable, KEYOUT, VALUEOUT> {
}
