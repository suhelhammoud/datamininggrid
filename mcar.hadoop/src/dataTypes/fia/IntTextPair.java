package dataTypes.fia;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
public class IntTextPair extends MyPair {

	public IntTextPair() {
		super(IntWritable.class,Text.class);
	}
	
	
	public IntTextPair copy(){
		return new IntTextPair(new IntWritable(((IntWritable)p1).get()),new Text((Text)p2 ));
	}
	
	public IntTextPair(IntWritable i,Text t) {
		this();
		p2=t;
		p1=i;
	}
	public void set(int i,String t){
		p2=new Text(t);
		p1=new IntWritable(i);
	}
	
	public IntTextPair(int i,String t) {
		this();
		set(i,t);
	}

	public static void main(String[] args) {
		IntTextPair a1,a2;
		a1=new IntTextPair(6,"2");
		a2=new IntTextPair(6,"5");
		
		System.out.println(a1+"\t"+ a1.compareTo(a2));
	}
}
