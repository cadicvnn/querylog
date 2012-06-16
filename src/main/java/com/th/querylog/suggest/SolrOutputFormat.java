package com.th.querylog.suggest;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SolrOutputFormat extends FileOutputFormat<Text, IntWritable> {
  private SolrWriter writer;

  public RecordWriter<Text, IntWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    writer = new SolrWriter(context.getConfiguration());

    return new RecordWriter<Text, IntWritable>() {
      public void write(Text key, IntWritable value) throws IOException,
          InterruptedException {
        writer.write(key.toString(), value.get());
      }

      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        writer.close();
      }

    };
  }
}