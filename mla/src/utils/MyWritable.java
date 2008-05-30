package utils;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.*;
import java.lang.reflect.Array;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

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
public class MyWritable implements Writable {
  private Class valueClass=IntWritable.class;
  private IntWritable[] values;

  
  public MyWritable() {
	  }
  public MyWritable( IntWritable[] values) {
	    this.values = values;
	  }


  public Class getValueClass() {
    return IntWritable.class;
  }

  public String[] toStrings() {
    String[] strings = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      strings[i] = values[i].toString();
    }
    return strings;
  }

  public Object toArray() {
    Object result = Array.newInstance(valueClass, values.length);
    for (int i = 0; i < values.length; i++) {
      Array.set(result, i, values[i]);
    }
    return result;
  }

  public void set(IntWritable[] values) { this.values = values; }

  public IntWritable[] get() { return values; }

  public void readFields(DataInput in) throws IOException {
    values = new IntWritable[in.readInt()];          // construct values
    for (int i = 0; i < values.length; i++) {
      IntWritable value = new IntWritable();
      value.readFields(in);                       // read a value
      values[i] = value;                          // store it in values
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values
    for (int i = 0; i < values.length; i++) {
    	System.out.print(","+values[i].get());
      //values[i].write(out);
      out.writeInt(values[i].get());
    }
    System.out.println();
  }

}

