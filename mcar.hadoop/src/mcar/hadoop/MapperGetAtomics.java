package mcar.hadoop;

import hdm.HRow;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import dataTypes.IntListWritable;

public class MapperGetAtomics extends MapReduceBase implements Mapper<IntWritable,IntListWritable,IntWritable,IntWritable> {
	public final static IntWritable ONE=new IntWritable(1);
	
	

	@Override
	public void map(IntWritable key, IntListWritable value,
			OutputCollector<IntWritable, IntWritable> output, Reporter reporter)
			throws IOException {
	
		
	}
	


}
