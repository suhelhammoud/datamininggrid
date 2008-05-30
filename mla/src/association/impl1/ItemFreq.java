package association.impl1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import utils.SetWritable;

public class ItemFreq implements Writable{

	public SetWritable item=new SetWritable();
	public IntWritable	freq=new IntWritable();
	
	public int size(){
		return item.size();
	}
	public ItemFreq(SetWritable sw, IntWritable f){
		this.item.addAll(sw);
		this.freq.set(f.get());
	}
	public ItemFreq(){
		
	}
	public void readFields(DataInput in) throws IOException {
		this.item.readFields(in);
		this.freq.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		this.item.write(out);
		this.freq.write(out);
	}
	
	public String toString(){
		return item.toString()+":"+freq.toString();
	}

}
