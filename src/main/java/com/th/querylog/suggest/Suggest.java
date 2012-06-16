package com.th.querylog.suggest;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Suggest extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Suggest(), args);
    System.exit(exitCode);
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    Path[] inputs = (Path[]) null;

    for (int i = 0; i < args.length; i++) {
      if ("-input".equals(args[i])) {
        i++;
        Path dir = new Path(args[i]);
        FileSystem fs = dir.getFileSystem(getConf());
        FileStatus[] fstats = fs.listStatus(dir, new PathFilter() {
          private final String regex = "^.*/user-ct-test-collection-\\d+.txt$";

          public boolean accept(Path path) {
            return path.toString().matches(regex);
          }
        });
        inputs = FileUtil.stat2Paths(fstats);
      }
    }

    if (inputs.length == 0) {
      usage();
    }

    return suggest(inputs, conf);
  }

  private void usage() {
    System.out.println("Suggest:");
    System.out.println("    -input INPUT DIR \t use INPUT as input resource");
  }

  private final int suggest(Path[] inputs, Configuration conf)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new Job(getConf(), "Suggest");
    job.setJarByClass(Suggest.class);

    job.setMapperClass(SuggestMaper.class);
    job.setCombinerClass(SuggestReducer.class);
    job.setReducerClass(SuggestReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(SolrOutputFormat.class);

    for (Path input : inputs) {
      FileInputFormat.addInputPath(job, input);
    }
    Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-"
        + new Random().nextInt());
    FileOutputFormat.setOutputPath(job, tmp);

    int returnValue = 0;
    try {
      returnValue = job.waitForCompletion(true) ? 0 : 1;
    } finally {
      FileSystem.get(job.getConfiguration()).delete(tmp, true);
    }

    return returnValue;
  }
}