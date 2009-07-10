package example;

import mcar.mapreduce.DataBag;

public class Driver {

	public static void main(String[] args) {
		int support =2;
		int confidence= (int)(0.4*Integer.MAX_VALUE );
		
		DataBag allItems=new DataBag();
		
		DataBag dataInit=InitData.runJob("data/example.txt",false);
		
		//one step in finding frequent items sets
		// map data to FrequentItem space
		DataBag items=ToItemSizeOne.runJob(dataInit,support,confidence,false);
		allItems.addAll(items);//size 1
		//System.out.println("iteration 1 size "+ items.size());
		
		DataBag lines= ToLines.runJob(items, false);//line 1
		
	
		
		
		items= ToItems.runJob(lines, support, confidence, false);
		allItems.addAll(items); //size 2
		

		System.out.println("iteration 2 size "+ items.size());
		lines=ToLines.runJob(items, false);
		
		

		items= ToItems.runJob(lines, support, confidence, false);
		allItems.addAll(items);// size 3
		
		

//		System.out.println("iteration 3 size "+ items.size());
//		lines=ToLines.runJob(items, true);
//		//// start finding rules
		
		
		lines=RulesToLines.runJob(allItems, true);
		for (Object object : lines) {
			System.out.println(object);
		}
		
		
		if(true)return;

	}
}
