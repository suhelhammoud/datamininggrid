package utils;


	import java.io.DataInput;
	import java.io.DataOutput;
	import java.io.IOException;
	import java.util.ArrayList;
	import java.util.HashMap;
	import java.util.HashSet;
	import java.util.Iterator;
	import java.util.List;
	import java.util.Set;
	import java.util.TreeSet;

	import org.apache.hadoop.io.VLongWritable;
	import org.apache.hadoop.io.Writable;
	import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import rules.SortDriver;
	
	public class SuperSetWritable extends TreeSet<SetWritable> implements Writable{

		static Logger log=Logger.getLogger(SuperSetWritable.class);
		
		public void readFields(DataInput in) throws IOException {
			clear();
			int sz=in.readInt();
			
			for (int i = 0; i < sz; i++) {
				SetWritable itemSet=new SetWritable();
				itemSet.readFields(in);
				add(itemSet);
			}


		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(size());
			Iterator<SetWritable> iter=this.iterator();
			while (iter.hasNext()) {
				SetWritable set=iter.next();
				//log.info("Goning to write SetWritable:"+set.toString());
				set.write(out);
				//iter.next().write(out);
			}
		}
		
		//needed for TextOutputFormat
		public String toString(){
			Iterator<SetWritable> iter=this.iterator();
			StringBuffer sb=new StringBuffer(iter.next().toString());
			while (iter.hasNext()) {
				sb.append("~"+ iter.next().toString());
				
			}
			return sb.toString();
		}
		
		public static void main(String[] args){
			SetWritable s1=new SetWritable();
			s1.add(1);
			s1.add(1);
			s1.add(2);
			
			
			SetWritable s2=new SetWritable();
			s2.add(1);
			s2.add(2);
			
			Set<SetWritable> set=new HashSet<SetWritable>();
			System.out.println(set.add(s1));
			System.out.println(set.add(s2));
			System.out.println(set.size());
			System.out.println("equal: "+(s1.equals(s2)));
			System.out.println("== "+(s1==s2));
			
			HashMap<SetWritable, Integer> map=new HashMap<SetWritable, Integer>();
			map.put(s1, 5);
			System.out.println("map(s2)="+map.get(s2));
			System.out.println("s1="+s1);
			System.out.println("s2="+s2);
			
			
			
		}

	}
