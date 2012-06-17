package com.th.querylog.related;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

public class SessionInputFormat extends FileInputFormat<LongWritable, Text> {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException, InterruptedException {
    context.setStatus(genericSplit.toString());
    return new LineRecordReader();
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for(FileStatus status : listStatus(job)) {
      splits.addAll(getSplitsForFile(job, status));
    }
    
    return splits;
  }

  private List<FileSplit> getSplitsForFile(JobContext job, FileStatus status) throws IOException {
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    long maxSize = getMaxSplitSize(job);
    long blockSize = status.getBlockSize();
    long splitSize = computeSplitSize(blockSize, minSize, maxSize);
    
    Configuration conf = job.getConfiguration();
    
    List<FileSplit> splits = new ArrayList<FileSplit> ();
    
    Path fileName = status.getPath();
    if(status.isDir()) {
      throw new IOException("Not a file: " + fileName);
    }
    FileSystem  fs = fileName.getFileSystem(conf);
    LineReader lr = null;
    try {
      FSDataInputStream in  = fs.open(fileName);
      lr = new LineReader(in, conf);
      Text line = new Text();
      long begin = 0;
      long length = 0;
      int num = -1;
      String prevAnonId = null;
      while ((num = lr.readLine(line)) > 0) {
        length += num;
        String anonId = getAnonId(line.toString());
        if(prevAnonId == null || !prevAnonId.equals(anonId)) {
          if(length > splitSize) {
            if(begin == 0) {
              splits.add(new FileSplit(fileName, begin, length -1 , new String[]{}));
            } else {
              splits.add(new FileSplit(fileName, begin - 1, length , new String[]{}));
            }
            begin += length;
            length = 0;
            prevAnonId = anonId;
          }
        }
      }
      if (length > 0) {
        splits.add(new FileSplit(fileName, begin, length, new String[]{}));
      }
  
    } finally {
      if (lr != null) {
        lr.close();
      }
    }
    
    return splits;
  }
  
  protected long getFormatMinSplitSize() {
    return 1;
  }
  
  protected static String getAnonId(String line) {
    int i = line.indexOf('\t');
    return line.substring(0, i); 
  }

}
