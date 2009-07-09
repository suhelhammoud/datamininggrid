package dataTypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;


import others.MyConstants;

public class IntListWritable extends ArrayList<Integer> implements WritableComparable,MyConstants {
	//protected List<Integer> data;
	
	public IntListWritable(int size) {
		super(size);
	}
	
	public IntListWritable(int[] arr){
		this(arr.length);
		for (int i : arr)add(i);
	}
	
	
	public IntListWritable(Collection<Integer> c){
		addAll(c);
	}
	
	public IntListWritable(){
		super();
	}
	
	public void setAll(int... d){
		clear();
		add2(d);
	}
	public void add2(int...d){
		for(int i:d)add(i);
	}

	public void set(List<Integer> data){
		clear();
		addAll(data);
	}
	

	public static IntListWritable fromBytes(byte[] b){
		ByteBuffer bb=ByteBuffer.wrap(b);
		int sz=b.length/SIZEOF_INT;
		IntListWritable data=new IntListWritable(sz);
		for (int i = 0; i < sz; i++)
			data.add(bb.getInt());
		return data;
	}
	
	public byte[] toBytes() {
		int sz=size()* SIZEOF_INT;
		ByteBuffer bb=ByteBuffer.allocate(sz);
		for (int i : this) {
			bb.putInt(i);
		}
		return bb.array();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		clear();
		int sz=in.readInt();
		for (int i = 0; i < sz; i++) {
			add(in.readInt());
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(size());
		for (int i : this) out.writeInt(i);
	}
	

	
	@Override
	public int compareTo(Object o) {
		IntListWritable other=(IntListWritable)o;
		int len1=size();
		int len2=other.size();
		int diff=0;
		for (int i = 0;	i < len1 && i<len2 ; i++) {
			diff=get(i)-other.get(i);
			if(diff !=0 )return diff;
		}
		return 0;
	}
//	@Override
//	public int compareTo(Object o) {
//		IntListWritable other=(IntListWritable)o;
//		int len1=size();
//		int diff=len1-other.size();
//		if(diff !=0)return diff;
//		for (int i = 0;	i < len1 ; i++) {
//			diff=get(i)-other.get(i);
//			if(diff !=0 )return diff;
//		}
//		return 0;
//	}
//	
	

	public static void main(String[] args) throws Exception{
	}
}


