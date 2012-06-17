package com.th.querylog.related;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RelatedReducer extends Reducer<Text, Text, Text, ArrayWritable> {
  protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,ArrayWritable>.Context context) throws IOException ,InterruptedException {
    Set<String> listValue = new HashSet<String>();
    for(Text value : values) {
      listValue.add(value.toString());
    }
    if(listValue.size() > 1) {
      context.getCounter("Transaction", "N-Size").increment(1L);
      ArrayWritable relateds = new ArrayWritable(listValue.toArray(new String[listValue.size()]));
      context.write(key, relateds);
    } else {
      context.getCounter("Transaction", "1-Size").increment(1L);
    }
  };
}
