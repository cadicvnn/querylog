package com.th.querylog.suggest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SuggestReducer extends
    Reducer<Text, IntWritable, Text, IntWritable> {
  protected void reduce(Text key, Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
    int count = 0;
    for (IntWritable value : values) {
      count += value.get();
    }

    context.write(key, new IntWritable(count));
  }
}