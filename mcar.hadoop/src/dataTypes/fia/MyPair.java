package dataTypes.fia;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;
//import org.apache.hadoop.hbase.util.Pair;
public class MyPair implements WritableComparable {

	private Class<? extends WritableComparable> c1;
	private Class<? extends WritableComparable> c2;

	public WritableComparable p1,p2;


	public MyPair(Class<? extends WritableComparable> c1,
			Class<? extends WritableComparable> c2) {
		if (c1 == null || c2 == null) { 
			throw new IllegalArgumentException("null valueClass"); 
		}    
		this.c1 = c1;
		this.c2 = c2;
	}

	

	@Override
	public void readFields(DataInput in) throws IOException {
	    p1 = (WritableComparable) WritableFactories.newInstance(c1);
	    p2 = (WritableComparable) WritableFactories.newInstance(c2);

		p1.readFields(in);
		p2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		p1.write(out);
		p2.write(out);
	}

	@Override
	public int compareTo(Object o) {
		MyPair other=(MyPair)o;
		int diff=p1.compareTo(other.p1);
		if(diff != 0)return diff;
		return p2.compareTo(other.p2);
	}


	public boolean equals(Object other){
		return other instanceof MyPair 
		&& p1.equals(((MyPair)other).p1 )
		&& p2.equals(((MyPair)other).p2 );
	}

	public int hashCode(){
		return p1.hashCode() * 17 +p2.hashCode();
	}

	@Override
	public String toString() {
		return "["+p1.toString()+","+p2.toString()+"]";
	}

}
