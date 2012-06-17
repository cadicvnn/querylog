package com.th.querylog.related;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.th.querylog.extract.AOLExtractor;
import com.th.querylog.extract.IExtractor;
import com.th.querylog.model.Click;
import com.th.querylog.model.Query;

public class RelatedMapper extends Mapper<LongWritable, Text, Text, Text> {
  private static final long   MAX_TRANSACTION_INTERVAL_LENGTH                         = 5 * 60 * 1000;
  private static final long   MAX_INACTIVE_INTERVAL_LENGTH                            = 24 * 60 * 60 * 1000;
  private static final long   MAX_TRANSACTION_TIME_WINDOW_LENGTH                      = 60 * 60 * 1000;
  private static final double MIN_LEVENSHTEIN_DISTANCE_SIMILARITY_FOR_RELATED_QUERIES = 1 / 3d;
  
  private IExtractor extractor;
  
  private Text currentTransactionKey;
  private Query previousQuery;
  private Date currentTransactionQueryTime;
  
  
  protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    extractor = new AOLExtractor();
  }
  
  protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,Text>.Context context) throws IOException ,InterruptedException {
    Query query = extractor.extract(value.toString());
    if (query != null) {
      if ((query instanceof Click)) {
        context.getCounter("Query Type", "Click").increment(1L);
      } else if(!"-".equals(query.getQuery())){
        if (previousQuery == null || query.getAnonId() != previousQuery.getAnonId()) {
          newTransaction(context, query);
        } else {
          long deltaQuery = query.getQueryTime().getTime() - previousQuery.getQueryTime().getTime();
          long deltaTransaction = query.getQueryTime().getTime() - currentTransactionQueryTime.getTime();
          
          if(deltaQuery <= MAX_TRANSACTION_INTERVAL_LENGTH && deltaTransaction <= MAX_TRANSACTION_TIME_WINDOW_LENGTH) {
          } else if(deltaQuery > MAX_INACTIVE_INTERVAL_LENGTH) {
            newTransaction(context, query);
          } else {
            double similarity = Utils.computeLevenshteinDistanceSimilarity(previousQuery.getQuery(), query.getQuery());
            if(similarity < MIN_LEVENSHTEIN_DISTANCE_SIMILARITY_FOR_RELATED_QUERIES) {
              newTransaction(context, query);
            }
          }
        }
        previousQuery = query;
        context.write(currentTransactionKey, new Text(query.getQuery()));
        context.getCounter("Query Type", "Query").increment(1L);
      }
      context.getCounter("Query Type", "All Query").increment(1L);
    }
  }

  private void newTransaction(
      Mapper<LongWritable, Text, Text, Text>.Context context, Query query) {
    currentTransactionQueryTime = query.getQueryTime();
    currentTransactionKey = new Text(query.getAnonId() + "-" + query.getQueryTime().toString());
    context.getCounter("Transaction", "All").increment(1L);
  };
  
}
