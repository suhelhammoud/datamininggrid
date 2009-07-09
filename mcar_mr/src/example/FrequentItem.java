
package example;

import org.apache.log4j.Logger;

import java.util.*;

//TODO change to HashMap later
public class FrequentItem extends TreeMap<Integer, List<Integer>> {
	
	private static final Logger logger = Logger.getLogger(FrequentItem.class);
	public static enum TAG{support,allOcc,minline,confidence};

	boolean isRule=false;
	int support,allOcc,minLine, confidence;
	


	public int getConfidene(){
		return confidence;
	}
	public int getSupport() {
		return support;
	}
	public void setSupport(int support) {
		this.support = support;
	}
	public int getAllOcc() {
		return allOcc;
	}
	public void setAllOcc(int allOcc) {
		this.allOcc = allOcc;
	}
	public int getMinLine() {
		return minLine;
	}
	public void setMinLine(int minLine) {
		this.minLine = minLine;
	}
	public FrequentItem() {
	}
	public void set(Map<Integer, List<Integer>> mp){
		clear();
		putAll(mp);
	}

	
	
 	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	
	@Override
	public String toString() {		
		StringBuffer sb=new StringBuffer("{");
		
		sb.append("isRule="+isRule+", Occ="+allOcc+", support="+support+", minLine="+minLine +"\n\t");
		for (Map.Entry<Integer,List<Integer>> e : entrySet()) {
			List<Integer> sorted=new ArrayList<Integer>(e.getValue());
			Collections.sort(sorted);
			sb.append(e.getKey()+":"+ sorted+ " , ");
		}
		sb.append("}");
		return sb.toString();
	}

	public void add(int label, int line){
		List<Integer> list=get(label);
		if(list==null){
			list=new ArrayList<Integer>();
			put(label,list);
			list.add(line);
			return;
		}else{
			if(! list.contains(line))
				list.add(line);
			else
				logger.error("contains "+label+", "+line);
		}

	}

	public void addArray(int label, int... arr){
		List<Integer> list=get(label);
		if(list==null){
			list=new ArrayList<Integer>(arr.length);
			put(label,list);
			for(int i: arr)list.add(i);
			return;
		}

		for (int i : arr){
			if(! list.contains(i))
				list.add(i);
			else
				logger.error("contains "+label+", "+i);
		}
	}


	public void addMap(Map<Integer, List<Integer>> map){
		for (Map.Entry<Integer, List<Integer>> e : map.entrySet()) {
			addList(e.getKey(),e.getValue());
		}
	}

	public void addList(int label,List<Integer> list) {
		List<Integer> local=get(label);
		if (local==null)
			put(label, list);
		else{
			for (int i : list){
				if(! local.contains(i))
					local.add(i);
				else
					logger.error("contains "+label+", "+i);
			}
		}
	}
	//TODO to delete later
	<T> boolean containsAny(List<T> list1,List<T> list2){
		HashSet<T> set1=new HashSet<T>(list1);
		return set1.removeAll(list2);
	}

	//max laber,allOccs, lowest line
	public int[] calc(){
		int max=-1;
		int min=Integer.MAX_VALUE;
		int counter=0;
		for (List<Integer> i : values()) {
			int sz=i.size();
			if (sz>max)max=sz;//support
			counter+=sz;//allOcc

			int ln=	Collections.min(i);
			if(ln < min)min=ln;//minLine
		}
		support=max;
		allOcc=counter;
		minLine=min;
		confidence=(int)(Integer.MAX_VALUE*(float) support/(float)allOcc);
		int[] result=new int[4];
		result[TAG.support.ordinal()]=max;
		result[TAG.allOcc.ordinal()]=counter;
		result[TAG.minline.ordinal()]=minLine;
		result[TAG.confidence.ordinal()]=confidence;
		return result;
	}

	public boolean isRule(){
		return isRule;
	}
	

	

	

	public static void main(String[] args) {
		FrequentItem map=new FrequentItem();
		map.addArray(1,new int[]{1,2,3});
		map.addArray(2,new int[]{221,22,23});

		map.addArray(1, 4,5);
		map.addArray(2,444,4444,4444);

		System.out.println(map);
		map.addArray(1,new int[]{5555551,255555,3});

		System.out.println(map);

	}
	public void setIsRule(boolean b) {
		isRule=b;		
	}


}
