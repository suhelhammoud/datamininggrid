package rules;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SortMapper extends MapReduceBase implements Mapper<FloatWritable,Text,FloatWritable,Text> {

	public void map(FloatWritable key, Text values,
			OutputCollector<FloatWritable,Text> output, Reporter reporter) throws IOException {
		output.collect(key, values);
	}

}
