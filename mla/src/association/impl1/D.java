package association.impl1;

import java.util.Date;

import org.apache.log4j.Logger;




public class D {
	static Logger log=Logger.getLogger(D.class);
	//public static enum myCounters {NUMBER_OF_ROWS,ROWS_LEFT} 


	public static void main(String[] args) {
		System.out.println("2 2 fixed2");
		String jobname="2 2 fixed2 ";
		int totalSize=10;
		double rSupport=0.2;
		double confidence=0.00;

//		int totalSize=990002;
//		double rSupport=0.00;
//		double confidence=0.00;

		int support=(int)(totalSize* rSupport);
		System.out.println("min support ="+ support);

		//support=2;

		String freqDir="data/freqs";

		//get 
		Param param=new Param();
		param.jobName=jobname;
		param.numOfMappers=2;
		param.numOfReduces=2;
		
		//TODO to be deleted later 
		//param=null;

		InputMapper.runJob("data/input_lined", "data/input", param);


		

		long t_start=D.tic();
		long t=D.tic();
		ZeroMapper.runJob("data/input", freqDir+"/1", support, param);
		System.out.println("ZerroMapper ,"+D.toc(t)+", "+D.toc(t_start));

		t=D.tic();
		//RemoveNotFrequentMapper.runJob("data/input","data/input_removed",freqDir, param);
		//System.out.println("RemoveNotFrequentMapper ,"+D.toc(t)+", "+D.toc(t_start));

		for(int i=2; ; i++){
			t=D.tic();
			int itemsLeft=IMapper.runJob("data/input", "data/freqs" , i, support, param);
			System.out.println("IMapper.runJob "+i+","+D.toc(t)+", "+D.toc(t_start));
			System.out.println("rows left "+itemsLeft );

			if(itemsLeft < 1)break;

		}

		//RMapper.runJob("data/freqs/*", "data/rules", totalSize, (float)confidence, param);
		//RSortMapper.runJob("data/rules", "data/rules_sorted", param);


	}


	public static long tic(){
		return new Date().getTime();
	}
	public static long toc(long tic){
		return (new Date().getTime())-tic;
	}

}
