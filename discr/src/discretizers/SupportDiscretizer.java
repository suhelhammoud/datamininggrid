package discretizers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.sun.org.apache.xml.internal.security.Init;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
//import weka.core.Instances;

public class SupportDiscretizer {



	List<Discret> data=new ArrayList<Discret>();
	List<SsfMap> mrg;

	public List<SSF> preprocess(){
		Collections.sort(data);

		Discret dd=data.get(0);
		SSF ssf=new SSF(dd.label,dd.numerical,dd.numerical,1);

		List<SSF> result=new ArrayList<SSF>();
		//result.add(new SSF(ssf));

		for(int i=1;i<data.size();i++){
			Discret d=data.get(i);
			if(d.label!=ssf.label){
				result.add(new SSF(ssf));
				ssf=new SSF(d.label,d.numerical,d.numerical,1);
			}else{
				ssf.freq++;
				ssf.stop=d.numerical;
			}
		}
		result.add(ssf);
		return result;
	}

	public List<SsfMap> merg2(List<SsfMap> sm){
		List<SsfMap> result=new ArrayList<SsfMap>();
		SsfMap fst=sm.get(0);

		for(int i=1;i<sm.size();i++){
			SsfMap item=sm.get(i);
			if(item.map.size()>1 || ! fst.map.keySet().equals(item.map.keySet())){
				result.add(new SsfMap(fst));
				fst=item;;
			}else{
				fst.add(item);
			}
		}
		result.add(fst);
		return result;
	}

	public List<SsfMap> preprocess2(){
		Collections.sort(data);

		Discret first=data.get(0);

		SsfMap fst=new SsfMap(first.numerical,first.label);
		List<SsfMap> result=new ArrayList<SsfMap>();

		for(int i=1;i<data.size();i++){
			Discret d=data.get(i);
			if(d.numerical!=fst.stop){
				result.add(new SsfMap(fst));
				fst=new SsfMap(d.numerical,d.label);
			}else{
				fst.add(d.numerical, d.label);
			}
		}
		result.add(fst);
		return result;
	}

	public Attribute getAttribute(String name){
		FastVector fv=new FastVector(mrg.size());

		for (SsfMap i : mrg) {
			//att.addStringValue(i.toString(true));
			fv.addElement(i.toString(true));
		}
		
		Attribute att=new Attribute(name,fv);
		System.out.println("att type "+att.type());
		
		return att;
	}
	public void discretize(int msupport,float mconfidence){
		List<SSF> result=preprocess();
		//now data is 1 merged
		Map<Integer, Integer> map=new LinkedHashMap<Integer, Integer>();

		List<SSF> section=new ArrayList<SSF>();
		SSF lastSsf=result.get(0);
		for (int i = 1; i < result.size(); i++) {

			SSF item=result.get(i);
			if(item.freq < msupport){
				Integer mapFreq=map.get(item.label);
				map.put(item.label, item.freq+ (mapFreq==null?0:mapFreq));
			}else{
				//get max map
				int occ=0;int max=Integer.MIN_VALUE;int lbl;
				for (Map.Entry<Integer, Integer> iter : map.entrySet()) {
					occ+=iter.getValue();
					if(iter.getValue() > max){
						max=iter.getValue();
						lbl=iter.getKey();
					}
				}

			}
		}

	}

	public List<SsfMap> mrgResult(int support,float confidence){
		List<SsfMap> result2=preprocess2();
		for (SsfMap ssfMap : result2) {
			System.out.println(ssfMap);
		}
		//System.out.println("*****222**********");
		List<SsfMap> result3=merg2(result2);
		for (SsfMap ssfMap : result3) {
			System.out.println(ssfMap);
		}


		mrg=SsfMap.merg(result3, support,confidence);	
		return mrg;
	}

	public void add(float numerical,int label){
		data.add(new Discret(numerical,label));
	}

	public int intMapValue(String v){
		float f=Float.valueOf(v);
		for (int j = 0; j < mrg.size(); j++) {
			SsfMap i=mrg.get(j);
			if(i.contains(f))
				return j;
		}
		
		return -1;
	}
	public String mapValue(String v){
		float f=Float.valueOf(v);
		for (SsfMap i : mrg) {
			if(i.contains(f))
				return i.toString(true);				
		}
		return "null";
	}

	public static void mapcontinous(String inFile,String outFile, int classIndex, float fsupport,float confidence) throws FileNotFoundException, IOException{
		Instances data=new Instances(new FileReader(inFile));
		int support=(int)Math.ceil(fsupport* data.numInstances());
		System.out.println("support ="+ support);
		if(classIndex== -1)classIndex=data.numAttributes()-1;
		data.setClassIndex(classIndex);//set the class
		Map<Integer,SupportDiscretizer> map=new TreeMap<Integer,SupportDiscretizer>();
		for (int i = 0; i < data.numAttributes(); i++) {
			if(data.attribute(i).isNumeric() && i != classIndex)
				map.put(i, new SupportDiscretizer());
		}
		int[] indexs=new int[map.size()];
		int counter=0;
		for (int i : map.keySet()) {
			indexs[counter++]=i;
		}


		//SupportDiscretizer disc=new SupportDiscretizer();
		for (int i = 0; i < data.numInstances(); i++) {
			Instance ins=data.instance(i);
			for (int j : indexs) {
				map.get(j).add((float)ins.value(j), (int)ins.value(classIndex));
			}
		}

		for (SupportDiscretizer iter : map.values()) {
			iter.mrgResult(support, confidence);
		}

		/////////////////////
		FastVector fv=new FastVector(data.numAttributes());
		for (int i = 0; i < data.numAttributes(); i++) {
			if(data.attribute(i).isNumeric())
				fv.addElement(map.get(i).getAttribute(data.attribute(i).name()));
			else
				fv.addElement(data.attribute(i));
		}
		Instances ndata=new Instances(data.relationName()+"_mapped",
				fv, data.numInstances());
		for (int i = 0; i < data.numInstances(); i++) {
			Instance ins=data.instance(i);
			double[] dd=new double[ins.numValues()];
			for (int j = 0; j < dd.length; j++) {
				dd[j]=ins.value(j);
			}
			for (int j : indexs) {
				SupportDiscretizer disc=map.get(j);
				String strj=String.valueOf(ins.value(j));
				int ii=disc.intMapValue(strj);
				dd[j]=ii;
			}
			ndata.add(new Instance(1, dd));
			
		}
		
		System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^");
		System.out.println(ndata);
		BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		 writer.write(ndata.toString());
		 writer.flush();
		 writer.close();
	}
	public static void main(String[] args) throws FileNotFoundException, IOException {

//		mapcontinous("data/data/iris.arff", "data/data/iris_mapped.arff", 4, 0.04f, 0.4f);
		mapcontinous("data/data/segment-challenge.arff", "data/data/segment-challenge_mapped2.arff", -1, 0.005f, 0.4f);
//		Instances data=new Instances(new FileReader("data/data/iris.arff"));
//		data.setClassIndex(4);//set the class
//
//		SupportDiscretizer disc=new SupportDiscretizer();
//		for (int i = 0; i < data.numInstances(); i++) {
//			Instance ins=data.instance(i);
//			disc.add((float)ins.value(0), (int)ins.value(4));
//		}
//
//		//		List<SSF> result=disc.preprocess();
//		//		for (SSF ssf : result) {
//		//			System.out.println(ssf);
//		//		}
//
//
//		int support=1;
//		float confidence=0.0f;
//
//		List<SsfMap> mrgResult=disc.mrgResult(support, confidence);
//		System.out.println("support= "+ support+" , confidence= "+confidence);
//		for (SsfMap ssfMap : mrgResult) {
//			System.out.println(ssfMap);
//		}
		//		System.out.println("777777777777777777");
		//		for (Discret i : disc.data) {
		//			System.out.println(i);
		//		}
	}

}
class SsfMap {
	final float start;
	float stop;
	int allOcc;
	Map<Integer,Integer> map;

	public SsfMap(SsfMap sm){
		this.start=sm.start;
		this.stop=sm.stop;
		this.allOcc=sm.allOcc;
		this.map=new TreeMap<Integer, Integer>(sm.map);
	}
	public SsfMap(float numeric,int label) {
		this.start=numeric;
		this.stop=numeric;
		map=new TreeMap<Integer, Integer>();
		map.put(label, 1);
		allOcc=1;
	}

	public static SsfMap add(SsfMap sm1,SsfMap sm2){
		SsfMap s1,s2,result;
		if(sm1.start < sm2.start ){
			s1=sm1;s2=sm2;
		}else{
			s1=sm2;s2=sm1;
		}
		result=new SsfMap(s1);
		result.add(s2);
		return result;
	}
	public void add(SsfMap sm){
		this.stop=sm.stop;
		for (Map.Entry<Integer, Integer> iter : sm.map.entrySet()) {
			Integer freq=map.get(iter.getKey());
			map.put(iter.getKey(), freq==null?iter.getValue():
				iter.getValue()+freq);
		}
		allOcc+=sm.allOcc;
	}
	public void add(float numeric,int label){
		stop=numeric;
		Integer freq=map.get(label);
		map.put(label, freq==null?1:freq+1);
		allOcc++;
	}

	public static List<SsfMap> merg(List<SsfMap> input,int support,float confidence){
		List<SsfMap> result=new ArrayList<SsfMap>();
		SsfMap fst=input.get(0);
		for (int i = 1; i < input.size(); i++) {
			SsfMap mrg=canMerg(fst,input.get(i), support, confidence);
			if(mrg ==null){
				result.add(fst);
				fst=new SsfMap(input.get(i));
			}else{
				fst=mrg;
			}
		}
		result.add(fst);
		return result;

	}
	public static SsfMap canMerg(SsfMap s1,SsfMap s2,int support,float confidence ){
		SsfMap s=SsfMap.add(s1, s2);
		//both not survived
		if(s1.getSupportl()< support && s2.getSupportl() < support)
			return s;

		
		if(s1.getSupportl()>= support && s2.getSupportl() >= support){
			//both survived
			
			if(s1.getConfidence()>=confidence || s2.getConfidence()>=confidence){//two rules
				if(s.getConfidence()< confidence)return null;

				if(s1.getConfidence() > s2.getConfidence()){
					Set<Integer> lbl=s1.getMaxLabels();
					lbl.retainAll(s.getMaxLabels());
					if(lbl.size()==0)return null;
					else return s;

				}else{
					Set<Integer> lbl=s2.getMaxLabels();
					lbl.retainAll(s.getMaxLabels());
					if(lbl.size()==0)return null;
					else return s;

				}
				//lbl1.retainAll(s.getMaxLabels());		
			}
		}else{	
			// one survived	
			if(s1.getSupportl()>support){
				if(s1.getConfidence()< confidence )return s;
				Set<Integer> lbl=s1.getMaxLabels();
				lbl.retainAll(s.getMaxLabels());
				if(lbl.size()==0)return null;
				else return s;
			}else{
				if(s2.getConfidence()< confidence )return s;
				Set<Integer> lbl=s2.getMaxLabels();
				lbl.retainAll(s.getMaxLabels());
				if(lbl.size()==0)return null;
				else return s;
			}

		}
		return null;

	}
	public SsfMap canMerg(SsfMap sm,int support,float confidence){
		SsfMap mrg=new SsfMap(this);
		mrg.add(sm);

		if(getSupportl()<support){
			if(sm.getSupportl()<support)
				return mrg;

			if(sm.getConfidence()<confidence)
				return mrg;

			return sm.canMerg(this, support, confidence);
		}

		if(getConfidence()<confidence){
			if(sm.getSupportl()<support)return mrg;
			if(sm.getConfidence() < confidence)return mrg;

			return sm.canMerg(this, support, confidence);
		}



		return null;
	}

	public float getConfidence(){
		return (float)getSupportl()/(float)allOcc;
	}
	public Set<Integer> getMaxLabels(){
		Set<Integer> result=new TreeSet<Integer>();
		int support=getSupportl();
		for (Map.Entry<Integer, Integer> iter : map.entrySet()) {
			if(iter.getValue()== support)
				result.add(iter.getKey());
		}
		return result;
	}
	public int getSupportl(){
		int result=0;
		for (int  iter : map.values()) {
			if(iter > result){
				result=iter;
			}
		}
		return result;
	}

	public boolean contains(float numeric){
		if(numeric >= start && numeric <=stop)
			return true;
		else return false;
	}

	public String toString(boolean compact){
		return start+"-"+stop;

	}
	@Override
	public String toString() {
		return toString(true)+": ("+allOcc+") "+map.toString();
	}
}
class SSF{
	float start;
	float stop;
	int freq;
	int label;
	public SSF(){

	}
	public SSF(int label,float start,  float stop,int freq) {
		this.label=label;
		this.start = start;
		this.freq = freq;
		this.stop = stop;
	}
	public SSF(SSF ssf) {
		this(ssf.label,ssf.start,ssf.stop,ssf.freq);
	}
	@Override
	public String toString() {

		return start+" - "+stop+" : "+label+" ("+freq+")";
	}
}

class Discret implements Comparable{
	float numerical;
	int label;

	public Discret(float numerical, int label) {
		super();
		this.numerical = numerical;
		this.label = label;
	}

	@Override
	public int compareTo(Object arg0) {
		Discret other=(Discret)arg0;
		int diff= (int)Math.signum(numerical-other.numerical);
		if (diff != 0)
			return diff;
		else
			return label-other.label;
	}

	@Override
	public boolean equals(Object obj) {
		if( obj instanceof Discret){
			Discret other=(Discret)obj;
			return numerical==other.numerical &&
			label==other.label;
		}else
			return false;
	}

	@Override
	public int hashCode() {
		return 31* new Float(numerical).hashCode()+
		label;
	}

	@Override
	public String toString() {
		return numerical+"\t"+label;
	}


}

