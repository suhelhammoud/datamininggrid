package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.log4j.Logger;

import rules.SortDriver;

public class SetWritable extends TreeSet<Integer> implements WritableComparable {

	static Logger log=Logger.getLogger(SetWritable.class);

	public void readFields(DataInput in) throws IOException {
		clear();
		int sz=in.readInt();
		for (int i = 0; i < sz; i++) {
			add(in.readInt());
		}		
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(size());
		Iterator<Integer> iter=this.iterator();
		//log.info("inside set writable:"+toString());
		while (iter.hasNext()) {
			out.writeInt(iter.next());
		}
	}
	
	public SetWritable(){
		super();
	}
	public SetWritable(Collection<Integer> c){
		super(c);
	}
	
	//needed for TextOutputFormat
	public String toString(){
		return MapTools.join(" ", this);
	}
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		SetWritable itm=(SetWritable)o;
		int sz=size();
		int diff=sz-itm.size();
		if (diff !=0)
			return diff;

		Iterator<Integer> iter1=iterator();		
		Iterator<Integer> iter2=itm.iterator();

		for (int i = 0; i < sz;i++) {
			diff=iter1.next()-iter2.next();
			if(diff != 0)
				return diff;
		}
		return 0;
	}

	public SetWritable merg(SetWritable set) {
		if(size() != set.size())return null;
		SetWritable result=new SetWritable();
		result.addAll(this);
		result.addAll(set);
		if(result.size() != size()+1)return null;
		return result;
	} 
	public static void main(String[] args){
		SetWritable s1=new SetWritable();
		s1.add(1);
		s1.add(3);
		s1.add(4);


		SetWritable s2=new SetWritable();
		s2.add(1);
		s2.add(4);
		s2.add(3);

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
		
		System.out.println(s1.merg(s2));


	}

	
	public boolean add(IntWritable e) {
		return super.add(e.get());
	}
		

	
}
