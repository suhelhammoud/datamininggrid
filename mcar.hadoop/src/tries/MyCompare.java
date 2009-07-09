package tries;


import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import dataTypes.IntListWritable;
import dm.Classifier;
import dm.Column;
import dm.Data;
import dm.DataMine;
import dm.Sccl;

public class MyCompare {

	
	
	public static void compareMyColumn(MyColumn m1,MyColumn m2){
		System.out.println("m1 size="+m1.size());
		System.out.println("m2 size="+m2.size());
		for (Map.Entry<List<Integer>, Sccl> iter1 : m1.entrySet()) {
			if(! m2.containsKey(iter1.getKey())){
				System.out.println("item only in m1 "+ MyColumn.full(iter1.getKey())+"\t"+iter1.getValue());
				continue;
			}
			Sccl sccl2=m2.get(iter1.getKey());
			if(iter1.getValue().compareTo(sccl2) != 0){
				System.out.println("not equal\n\t\t "+iter1.getValue()
						+"\n\t\t"+ sccl2);
			}
		}
	}
	public static void testMyColumn() {
		MyColumn m1=new MyColumn("data/items");
		MyColumn m2=MyColumn.getMyColumn("data/in/arff/00.arff", 3);
		compareMyColumn(m1, m2);
		System.out.println("-------------------------");
		compareMyColumn(m2, m1);
		
	}
	public static void main(String[] args) {
		testMyColumn();
//		MyColumn m2=MyColumn.getMyColumn("data/in/arff/00.arff", 3);
//		System.out.println(m2);
	}
}
