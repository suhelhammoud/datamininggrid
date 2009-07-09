package init;

import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

import jruby.objectweb.asm.tree.IntInsnNode;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.mapred.JobConf;

import others.MyConstants;
import others.Tools;

import dataTypes.IntListWritable;
import dataTypes.IntMap;
import dm.DataMine;

public class Driver implements MyConstants {
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger.getLogger(Driver.class);

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		System.out.println("start 2");
		int iteration=1;
		int label=4;
		int support=3;
		float confidence=0.40f;
		
		String input_dir="data/in/input";
		String label_dir="data/labels";
		String items_dir="data/items";
		String lines_dir="data/lines";
		
		FileSystem fs = FileSystem.get(new JobConf());
		fs.delete(new Path(lines_dir), true);
		fs.delete(new Path(items_dir), true);
		
		//initData();
		//ToLabels.run(input_dir,label_dir,label);		
		
		ToItemsAtomic.run( input_dir, items_dir+"/1",label,support,confidence);
//				
		ToLines.run(items_dir,lines_dir,label_dir,1);

		
		//ToItems.run(lines_dir,items_dir,3,label,support,confidence);

		//if(true)return;
		for (iteration=2; iteration < 5; iteration++) {
			System.out.println("iter "+ iteration);
			logger.error("------------rrr------iteration "+iteration);
			long[] result=
				ToItems.run(lines_dir,items_dir,iteration,label,support,confidence);
			
			logger.error(Arrays.toString(result));
			if(result[0]==0)break;

			ToLines.run(items_dir,lines_dir,label_dir,iteration);
			
		}
	}
	
	
	public static void initData()  {
		try {

			FileSystem fs = FileSystem.get(new JobConf());
			fs.delete(new Path("data/in/lined"), true);
			fs.delete(new Path("data/in/map"), true);
			fs.delete(new Path("data/in/input"), true);
			
			Tools.addLines("data/in/raw", "data/in/lined");

			MapItemsToLowestLine.run("data/in/lined", "data/in/map");

			ReplaceItemsWithLines.run("data/in/lined", "data/in/input", "data/in/map");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	private static void run(int label,int support,float confidence) throws IOException {
		JobConf job = new JobConf();
	
		int iteration=1;
	
		
		String input_dir="data/in/input";
		String label_dir="data/labels";
		String items_dir="data/items";
		String lines_dir="data/lines";
		

		
		

	
		

		System.out.println("---------------------------------iteration  "+ 1);		
		ToLines.run(items_dir,lines_dir,label_dir,1);

		for (iteration=2; iteration < 5; iteration++) {
			System.out.println("---------------------------------iteration  "+
					iteration);

			long[] result=ToItems.run(lines_dir,items_dir,
					iteration,label,support,confidence);
			System.out.println(Arrays.toString(result));
			if(result[0]==0)break;

			ToLines.run(items_dir,lines_dir,label_dir,iteration);
			
			
		}
		

		System.out.println("Done ");
		
	}
}
