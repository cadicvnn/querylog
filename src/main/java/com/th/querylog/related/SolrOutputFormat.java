package com.th.querylog.related;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SolrOutputFormat extends FileOutputFormat<Text, ArrayWritable> {
  private SolrWriter writer;

  public RecordWriter<Text, ArrayWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    writer = new SolrWriter(context.getConfiguration());

    return new RecordWriter<Text, ArrayWritable>() {
      public void write(Text key, ArrayWritable value) throws IOException,
          InterruptedException {
        writer.write(key.toString(), value.toStrings());
      }

      public void close(TaskAttemptContext context) throws IOException,
          InterruptedException {
        writer.close();
      }

    };
  }
}