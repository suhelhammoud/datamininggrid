package rules;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import utils.MapTools;
import utils.PowerSet;
import utils.SetWritable;


public class RulesMapper extends MapReduceBase implements
Mapper<Text, IntWritable, Text, Text> {
	
	public void map(Text key, IntWritable values,
			OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
		
		String[] items=key.toString().split(" ");
		int freq=values.get();
		
		System.out.println(key.toString()+","+values.toString());

		Rule rule=new Rule(key.toString(),0,"",freq);
		rule.outMapper(output);
		System.out.println(rule.toString());
		PowerSet<String> ps=new PowerSet<String>(items);
		
		for (Set<String> set : ps) {
			String outleft=MapTools.join(" ", set);

			
			
			String outRight=Rule.complement(items, set);
			
			rule=new Rule(outleft,0,outRight,freq);
			rule.outMapper(output);
			System.out.println(rule.toString());
		}
		
	}
	
	//items length >0 

}
