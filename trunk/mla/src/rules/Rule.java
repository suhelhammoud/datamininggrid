package rules;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import utils.MapTools;

public class Rule {
	public static String FS=" ";

	private static int TOTAL;
	private static float CONFIDENCE=0;
	String[] left;
	String[] right;
	int	freq;
	int freqLeft;

	float confidence;
	float rSupport;

	public static void setTotal(int total){
		Rule.TOTAL=total;
	}
	public static void setCONFIDENCE(float CONFIDENCE){
		Rule.CONFIDENCE=CONFIDENCE;
	}
	public Rule(String left,int freqLeft, String right,int freq){
		this.left=left.split(FS);
		this.right=right.split(FS);
		this.freq=freq;
		this.freqLeft=freqLeft;
	}
	
	public boolean isPassed(){
		rSupport=(float)freq/(float)TOTAL;
		confidence=(float)freq/(float)freqLeft;
		System.out.println("confidence "+confidence);
		
		if (confidence > CONFIDENCE)
			return true;
		else 
			return false;
	}
	public static Rule getRule(String key, String value){
		String[] arr=value.split(":");
		System.out.println("key="+key+", value"+value);
		int f=Integer.valueOf(arr[1]);
		Rule rule=new Rule(key,0,arr[0],f);
		return rule;
	}
	
	public String toString(){
		return toStringLeft()+"\t-->\t"+toStringRight()+":\t"+confidence+":\t"+freq;
	}
	public String toStringLeft(){
		return MapTools.join(FS, left);
	}
	public String toStringRight(){
		return MapTools.join(FS, right);
	}
	public void outMapper(OutputCollector<Text, Text> output)
	throws IOException{
		output.collect(new Text(toStringLeft()), new Text(toStringRight()+":"+freq));
	}

		
	public void out(OutputCollector<FloatWritable, Text> output)
	throws IOException{
		float rank=getRank();
		//String outText=MapTools.join(FS, left)+"--->"+MapTools.join(FS, right)+":"+freqLeft+":"+confidence+":"+rSupport;
		output.collect(new FloatWritable(rank), new Text(toString()));
	}


	public float getRank(){
		float result=0;

		result+=confidence;
		result*=1000;
		result=(int)result;

		result+=rSupport;
		result*=1000;
		result=(int)result;

		float r3=1-(float)1/(left.length+right.length);

		result+=r3;
		return -result;
	}
	public static String complement(String[] items,Collection<String> c){
		StringBuffer sb=new StringBuffer();
		for (int i = 0; i < items.length; i++)
			if(! c.contains(items[i]))
				sb.append(items[i]+FS);
		
		
		return sb.toString().trim();
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
