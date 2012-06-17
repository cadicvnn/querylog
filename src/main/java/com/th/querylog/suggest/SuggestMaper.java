package com.th.querylog.suggest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.th.querylog.extract.AOLExtractor;
import com.th.querylog.extract.IExtractor;
import com.th.querylog.model.Click;
import com.th.querylog.model.Query;

public class SuggestMaper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private IExtractor extractor;
  private static final IntWritable ONE = new IntWritable(1);

  protected void setup(
      Mapper<LongWritable, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
    extractor = new AOLExtractor();
  }

  protected void map(LongWritable key, Text value,
      Mapper<LongWritable, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
    Query query = extractor.extract(value.toString());
    if (query != null) {
      if (!(query instanceof Click)) {
        context.getCounter("Query Type", "Query").increment(1L);
        context.write(new Text(query.getQuery()), ONE);
      } else {
        context.getCounter("Query Type", "Click").increment(1L);
      }
      context.getCounter("Query Type", "All Query").increment(1L);
    }
  }
}