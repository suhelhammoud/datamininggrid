package mcar.mapreduce;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class JobConf implements Reporter{
	String jobName;
	Map<String, Object> map=new HashMap<String, Object>();
	Counters counters=new Counters();

	private Class<? extends Mapper> cMapper;
	private Class<? extends Reducer> cReducer;
	//private Class<MapReduceBase> cMapBase;
	//private Class<MapReduceBase> cReduceBase;

	boolean verbose=false;
	public void setVerbose(boolean verbose){
		this.verbose=verbose;
	}
	
	DataBag _input;
	DataBag _output;
	public void set(String key,String value){
		map.put(key, value);
	}

	public <K extends Comparable,V> void set_input(Map<K,V> c){
		_input=new DataBag();
		for (Map.Entry<K, V> e : c.entrySet()) {
			_input.add(new KeyValue(e.getKey(),e.getValue()));
		}
	}
	public <V> void set_input(DataBag c){
		
		_input=new DataBag();
			int count=0;
			for (Iterator<V> iterator = c.iterator(); iterator.hasNext();) {
				V v = (V) iterator.next();
				_input.add(new KeyValue((KeyValue)v));
				count++;
			}
	}

	public void set_input(String fileName){
		_input=new DataBag();
		try {
	        BufferedReader in = new BufferedReader(new FileReader(fileName));
	        String str;
	        int counter=1;
	        while ((str = in.readLine()) != null) {
	            KeyValue kv=new KeyValue(counter,str);
	            _input.add(kv);
	            counter++;
	        }
	        in.close();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	}

//	public void set_input(DataBag _input){
//		this._input=_input;
//	}
	public DataBag get_input() {
		return _input;
	}
	public void set_output(DataBag _output){
		this._output=_output;
	}
	public DataBag get_output() {
		return (DataBag)_output;
	}

	public String get(String key,String defaultValue){
		String result=(String) map.get(key);
		if(result==null)return defaultValue;
		else return String.valueOf(result);
	}
	public String get(String key){
		return get(key,null);
	}
	public void setInt(String key,int value){
		map.put(key, String.valueOf(value));
	}
	public int getInt(String key, int defaultValue){
		String result=(String) map.get(key);
		if(result==null)return defaultValue;
		else return Integer.valueOf(result);
	}
	public void setBoolean(String key,boolean b){
		map.put(key, String.valueOf(b));
	}
	public boolean getBoolean(String key,boolean defaultValue){
		String result=(String) map.get(key);
		if(result==null)return defaultValue;
		else return Boolean.valueOf(result);
	}
	public float getFloat(String key,float defaultValue){
		String result=(String) map.get(key);
		if(result==null)return defaultValue;
		else return Float.valueOf(result);
	}
	public void setJobName(String name){
		this.jobName=name;
	}
	public String getJobName(){
		return this.jobName;
	}

	public void setMapperClass(Class<? extends Mapper> cMapper){
		this.cMapper=cMapper;
		//this.cMapBase=(Class<MapReduceBase>) cMapper;
	}
	public Class<? extends Mapper> getMapperClass(){
		return cMapper;
	}
	public void setReducerClass(Class<? extends Reducer> cReducer){
		this.cReducer=cReducer;
		//this.cReduceBase=(Class<MapReduceBase>) cReducer;

	}
	public Class<? extends Reducer> getReducerClass(){
		return cReducer;
	}
	/////counters 
	public Counters getCounters() throws IOException {
		return counters;
	}
	@Override
	public Counters.Counter getCounter(String group, String name) {
		return counters.findCounter(group, name);
	}

	@Override
	public void incrCounter(Enum key, long amount) {
		counters.findCounter(key).increment(amount);

	}

	@Override
	public void incrCounter(String group, String counter, long amount) {
		counters.findCounter(group, counter).increment(amount);
	}

	@Override
	public void progress() {}

	@Override
	public void setStatus(String s) {}


	public DataBag run(){
		MapReduceBase mb=new MapReduceBase();
		//mb = cMapBase.newInstance();
		mb.init(this);//(this);
		mb.run();
		
		return _output;
	}

}