package dataTypes.fia;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Two implements WritableComparable {

	public int p1=0;
	public int p2=0;
	


	public Two() {
	}

	public Two(int p1, int p2) {
		this.p1 = p1;
		this.p2 = p2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		p1=in.readInt();
		p2=in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(p1);
		out.writeInt(p2);
	}

	@Override
	public int compareTo(Object o) {
		Two other=(Two)o;
		int diff=p1-other.p1;
		if(diff != 0)return diff;
		return p2-other.p2;
	}


	public boolean equals(Object other){
		return other instanceof Two 
		&& p1==((Two)other).p1 
		&& p2==((Two)other).p2;

	}

	public int hashCode(){
		return p1 * 17 +p2;
	}

	@Override
	public String toString() {
		return "<"+p1+","+p2+">";
	}

}
