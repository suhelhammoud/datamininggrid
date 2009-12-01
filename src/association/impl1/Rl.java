package association.impl1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerRepository;

import utils.MapTools;
import utils.SetWritable;

public class Rl implements WritableComparable{
	static Logger log=Logger.getLogger(Rl.class);
	public static String FS=" ";

	private static float CONFIDENCE=0;
	private static int SUPPORT=0;
	SetWritable left=new SetWritable();
	SetWritable right=new SetWritable();
	int	freq;
	int freqLeft;

	float confidence(){
		return (float)freq/(float)freqLeft;
	}
	float rSupport;

	public static void setCONFIDENCE(float CONFIDENCE){
		Rl.CONFIDENCE=CONFIDENCE;
	}
	public static void setSUPPORT(int SUPPORT){
		Rl.SUPPORT=SUPPORT;
	}

	public Rl(){
		
	}
	public Rl(SetWritable left,int freqLeft, SetWritable right,int freq){
		//log.info(left+":"+freqLeft+"-->"+right+":"+freq);
		this.left.addAll(left);
		this.right.addAll(right);
		this.freq=freq;
		this.freqLeft=freqLeft;
	}
	
	public boolean isPassed(){
		if (confidence() > CONFIDENCE)
			return true;
		else 
			return false;
	}
	
	public String toString(){
		return left.toString()+"\t-->\t"+right.toString()+":\t"+confidence()+":\t"+freq;
	}

	public void outRMapper(OutputCollector<Text, Text> output)
	throws IOException{
		//output.collect(new Text(toStringLeft()), new Text(toStringRight()+":"+freq));
	}


	public static SetWritable complement(SetWritable items,Collection<Integer> c){
		SetWritable result=new SetWritable();
		for (Integer atomic : items) {
			if ( ! c.contains(atomic))result.add(atomic);
		}
		return result;
	}

	public int size(){
		return left.size()+right.size();
	}
	public int compareTo(Object o) {
		Rl rl=(Rl)o;
		float d=confidence()-rl.confidence();
		if(d !=0) return  (int)Math.signum(d);
		
		int diff=freq-rl.freq;
		if (diff != 0) return diff;
		
		diff = size()-rl.size();
		if (diff != 0)return diff;
		
		return left.compareTo(rl.left);

	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		left.readFields(in);
		freqLeft=in.readInt();
		right.readFields(in);
		freq=in.readInt();
		
	}
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		left.write(out);
		out.writeInt(freqLeft);
		right.write(out);
		out.writeInt(freq);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		float f=1222.44f;
		System.out.println((int)f);	
		System.out.println((float)1/(3+1));

	}

}
