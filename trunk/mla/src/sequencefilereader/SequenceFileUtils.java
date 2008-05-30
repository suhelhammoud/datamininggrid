/*
 * Copyright 2008 Last.fm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package sequencefilereader;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * Utilities for handling various sequence files (map files, sequence files, set files etc.).
 */
public final class SequenceFileUtils {

  /**
   * Due to hadoop deprecating certain methods I've added
   * this helper one to take in an array of FileStatus and return
   * and array of Path. 
   * 
   */
  public static Path[] getPathsFromFileStatus(FileStatus[] fileStatus) {
    if(fileStatus == null) {
      return new Path[] {};
    }
    
    Path[] result = new Path[fileStatus.length];
    for (int i = 0; i < fileStatus.length; i++) {
      result[i] = fileStatus[i].getPath();
    }
    
    return result;
  }

  /**
   * Reads the contents of a SequenceFile into a human-readable String using a default configuration object.
   * 
   * @param inputPath Path to the file to be read.
   * @return The contents of the file as a String.
   * @throws IOException If an error occurs reading the file.
   */
  public static String readSequenceFile(Path inputPath) throws IOException {
    return readSequenceFile(inputPath, new Configuration());
  }
  
  /**
   * Reads the contents of a SequenceFile into a human-readable String using a default configuration object.
   * The number of lines read in will be limited to the value set by the lineCount variable, 
   * if this is >= 0, all lines will be read.
   * 
   * @param inputPath Path to the file to be read.
   * @param lineCount Number of lines to read from the top of the file. 
   * @return The contents of the file as a String.
   * @throws IOException If an error occurs reading the file.
   */
  public static String readSequenceFileTop(Path inputPath, int lineCount) throws IOException {
    return readSequenceFileTop(inputPath, lineCount, new Configuration());
  }
  
  /**
   * Reads the contents of a SequenceFile into a human-readable String. The number of lines read in will be 
   * limited to the value set by the lineCount variable, if this is >= 0 , all lines will be read.
   * 
   * @param inputPath Path to the file to be read.
   * @param lineCount Number of lines to read from the top of the file.
   * @param conf Configuration object to use to get file system.
   * @return The contents of the file as a String.
   * @throws IOException If an error occurs reading the file.
   */
  public static String readSequenceFileTop(Path inputPath, int lineCount, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path[] files = null;
    FileStatus fileStatus = fs.getFileStatus(inputPath);
    if (fileStatus.isDir()) { // if a dir is passed in, list contents of each file in dir
      files = getPathsFromFileStatus(fs.listStatus(inputPath));
    } else { // we just have a single file
      files = new Path[] { inputPath };
    }

    StringBuffer result = new StringBuffer();
    for (Path inputFile : files) {
      fileStatus = fs.getFileStatus(inputFile);
      if (!fileStatus.isDir()) { // ignore subdirs for now, only process files
        SequenceFile.Reader reader = null;
        try {
          reader = new SequenceFile.Reader(fs, inputFile, conf);
          WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
          Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
          result.append("file: " + inputFile + ", keyClass: " + key.getClass().getName() + ", valueClass: "
              + value.getClass().getName() + "\n");
          int linesRead = 0;
          while (reader.next(key, value)) {
            if (lineCount>0 && linesRead>=lineCount) {
              break;
            }
            String valueString = value.toString();
            result.append(key.toString().trim() + "\t" + valueString);
            // for pretty output, put a newline between records which don't have one
            if (!valueString.endsWith("\n")) {
              result.append("\n");
            }
            linesRead++;
          }
        } finally {
          if (reader != null) {
            reader.close();
          }
        }
      }
    }
    return result.toString();
  }

  /**
   * Reads the contents of a SequenceFile into a human-readable String.
   * 
   * @param inputPath Path to the file to be read.
   * @return The contents of the file as a String.
   * @param conf Configuration object to use to get file system.
   * @throws IOException If an error occurs reading the file.
   */
  public static String readSequenceFile(Path inputPath, Configuration conf) throws IOException {
    return readSequenceFileTop(inputPath, -1, conf);
  }

}
