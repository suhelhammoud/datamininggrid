package dataTypes;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/** 
 * A Writable for arrays containing instances of a class. The elements of this
 * writable must all be instances of the same class. If this writable will be
 * the input for a Reducer, you will need to create a subclass that sets the
 * value to be of the proper type.
 *
 * For example:
 * <code>
 * public class IntArrayWritable extends ArrayWritable {
 *   public IntArrayWritable() { 
 *     super(IntWritable.class); 
 *   }	
 * }
 * </code>
 */
public class ListWritable implements Writable {
  private Class<? extends Writable> valueClass;
  private List<Writable> values;

  public ListWritable(Class<? extends Writable> valueClass) {
    if (valueClass == null) { 
      throw new IllegalArgumentException("null valueClass"); 
    }    
    this.valueClass = valueClass;
    values=new ArrayList<Writable>();
  }

  public ListWritable(Class<? extends Writable> valueClass, Writable start) {
    this(valueClass);
    this.values.add(start);
  }

  public boolean add(Writable w){
	  return values.add(w);
  }
  
  public ListWritable(String[] strings) {
    this(UTF8.class);
    for (int i = 0; i < strings.length; i++) {
      values.add(new UTF8(strings[i]));
    }
  }

  public Class getValueClass() {
    return valueClass;
  }

  @Override
	public String toString() {
		return values.toString();
	}
  
  public String[] toStrings() {
    String[] strings = new String[values.size()];
    for (int i = 0; i < values.size(); i++) {
      strings[i] = values.get(i).toString();
    }
    return strings;
  }

  public Object toArray() {
    Object result = Array.newInstance(valueClass, values.size());
    for (int i = 0; i < values.size(); i++) {
      Array.set(result, i, values.get(i));
    }
    return result;
  }

  public void set(List<Writable> values) { this.values = values; }

  public List<Writable> get() { return values; }

  public void readFields(DataInput in) throws IOException {
    values = new ArrayList<Writable>(in.readInt());          // construct values
    for (int i = 0; i < values.size(); i++) {
      Writable value = WritableFactories.newInstance(valueClass);
      value.readFields(in);                       // read a value
      values.add(value);                          // store it in values
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(values.size());                 // write values
    for (int i = 0; i < values.size(); i++) {
      values.get(i).write(out);
    }
  }

}

