package mcar.mapreduce;



import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;



public class MapReduceBase {

	JobConf job;
	Reporter reporter;
	//List<KeyValue<K extends Comparable, V>>
	Mapper mapper;
	Reducer reducer;


	DataBag input;
	DataBag output;
	DataBag mprd;

	OutputCollector mapCollector;
	OutputCollector reduceCollector;


	public MapReduceBase(){};
	
	public void init(JobConf job) {

		try {
			this.job=job;
			this.reporter=job;
			this.input=job.get_input();
			this.mprd=new DataBag();

			mapper=(Mapper) job.getMapperClass().newInstance();
			((MapReduceBase)mapper).configure(job);
			
			mapCollector=new MyOutputCollector(mprd);


			Class cReduce=job.getReducerClass();
			if(cReduce!=null){
				output=new DataBag();
				reducer=(Reducer)job.getReducerClass().newInstance();
				((MapReduceBase)reducer).configure(job);

				reduceCollector=new MyOutputCollector(output);
			}else{
				output=mprd;
			}
			job.set_output(output);

		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public <V> void run(){
		MyOutputCollector.verbose=job.verbose;
		if(job.verbose)System.out.println("\n------------------start mapping -------------------");
		
		try {
			for (Object ii : input) {
				
//				KeyValue<Comparable, V> i=(KeyValue<Comparable, V>)ii;
				KeyValue i=(KeyValue)ii;
				if(job.verbose)
					System.out.println(mapper.getClass().getName()
							+".map("+i.key+" ,"+i.value+") ");

				mapper.map(i.key, i.value, mapCollector, job);
				
			}
			
			if(job.verbose)System.out.println("------------------finished mapping -------------------");
			if(reducer==null){
				return;
			}
			if(job.verbose)System.out.println("\n------------------start reducing -------------------");

			Collections.sort(mprd);
			Iterator<KeyValue> iter=mprd.iterator();
			if(!iter.hasNext())return;
			
			KeyValue item=iter.next();
			DataBag list=new DataBag();

			while(iter.hasNext()){
//				list.clear();
//				list.add((V) item.value);
				while(iter.hasNext()){
					KeyValue item2=iter.next();
					if(item2.key.compareTo(item.key)!=0){
						if(job.verbose)
							System.out.println(reducer.getClass().getName()
									+".reduce("+item.key+" ,"+list+")");						
						reducer.reduce(item.key, list.iterator(), reduceCollector, job);
						list.clear();
						item=item2;
						list.add((V)item.value);
						break;
					}
					list.add((V) item2.value);
				}
			};
			reducer.reduce(item.key, list.iterator(), reduceCollector, job);

			if(job.verbose)System.out.println("------------------finshed reducing -------------------");
		} catch (IOException e) {
			e.printStackTrace();
		}


	}
	



	public void configure(JobConf job) {
	}
	

	enum  ec {c1,c2};
	public static void main(String[] args) {

		Counters cntrs=new Counters();
		Counters.Counter c=cntrs.findCounter(ec.c1);
		c.increment(3);
		System.out.println(cntrs);
	}
}



class MyOutputCollector <K,V> implements OutputCollector<Comparable<K>, V>{
	
	public static boolean verbose=false;
	//List<KeyValue<Comparable<K>, V>> list;
	DataBag list;

//	public MyOutputCollector(List<KeyValue<Comparable<K>, V>> list){
	public MyOutputCollector(DataBag list){
		this.list=list;
	}

	@Override
	public void collect(Comparable<K> key, V value) throws IOException {
		if(verbose)
			System.out.println("output.collect("+key+", "+value+")");
		list.add(new KeyValue(key,value));
	}

	
}