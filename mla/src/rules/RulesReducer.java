package rules;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class RulesReducer extends MapReduceBase implements Reducer<Text, Text, FloatWritable, Text> {
	static int total;
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<FloatWritable,Text> output, Reporter reporter) throws IOException {

		Rule basicRule=null;

		List<Rule> rules=new ArrayList<Rule>();
		while(values.hasNext()){
			Text v=values.next();
			Rule rule=Rule.getRule(key.toString(), v.toString());
			if(rule.toStringRight().equals("")){
				basicRule=rule;
				rule.freqLeft=rule.freq;
				break;
			}
			rules.add(rule);
		}
		for (Rule rule : rules) {
			rule.freqLeft=basicRule.freqLeft;
			if (rule.isPassed()){
				rule.out(output);
				System.out.println(rule.toString());
			}

		}

		while (values.hasNext()) {
			Text v=values.next();
			Rule rule=Rule.getRule(key.toString(), v.toString());
			rule.freqLeft=basicRule.freqLeft;
			if (rule.isPassed()){
				rule.out(output);
				System.out.println(rule.toString());
			}
		}
	}


	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
		super.configure(job);
		Rule.setTotal(job.getInt("TOTAL", 9));
		Rule.setCONFIDENCE(job.getFloat("CONFIDENCE", 0));
		
	}

}
