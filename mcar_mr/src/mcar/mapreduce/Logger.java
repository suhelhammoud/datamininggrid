package mcar.mapreduce;

import java.util.HashMap;
import java.util.Map;

public class Logger {
	String className;
	
	private Logger(String className){
		this.className=className;
	}
	 public static Logger getLogger(Class c){
		 String className=c.getName();
		return new Logger(className);
		 
	 }
	 
	 public void debug(Object message){
		 System.out.println(className+" "+ message);
	 }
	 public void info(Object message){
		 System.out.println(className+" "+ message);
	 }
	 public void warn(Object message){
		 System.out.println(className+" "+ message);
	 }
	 public void fatal(Object message){
		 System.out.println(className+" "+ message);
	 }
	 public void error(Object message){
		 System.out.println(className+" "+ message);
	 }

}
