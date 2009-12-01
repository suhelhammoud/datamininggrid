package hasim;

import java.util.ArrayList;
import java.util.List;

import dfs.HSplit;
public class HJobConf {
	public int mapperTaskVirtualMemeory=100;
	public int reducerTaskVirtualMemeory=100;
	public int combinerTaskVirutalMemory=100;
	public int shuffleTaskVirutalMemory=100;
	
	List<HSplit> splits=new ArrayList<HSplit>();
	
	public int numberOfMappers=10;
	public int numberOfReducers=2;
	

}
