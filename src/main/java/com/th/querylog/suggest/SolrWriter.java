package com.th.querylog.suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;

public class SolrWriter {
  private static final String SOLR_URL = "solr_url";
  private static final String COMMIT_SIZE = "commitSize";
  private static final String QUERY_FIELD = "query";
  private static final String COUNT_FIELD = "count";
  private SolrServer solr;
  private int commitSize = 1000;
  private final List<SolrInputDocument> inputDocs = new ArrayList<SolrInputDocument>();

  public SolrWriter(Configuration conf) throws IOException {
    solr = new HttpSolrServer(conf.get(SOLR_URL, "http://hmaster87:8983/solr"));
    commitSize = conf.getInt(COMMIT_SIZE, 1000);
  }

  public void write(String query, int count) throws IOException {
    SolrInputDocument inputDoc = new SolrInputDocument();
    inputDoc.addField(QUERY_FIELD, query);
    inputDoc.addField(COUNT_FIELD, Integer.valueOf(count));
    inputDocs.add(inputDoc);

    if (inputDocs.size() >= commitSize)
      update();
  }

  private void update() throws IOException {
    UpdateRequest req = new UpdateRequest();
    req.add(inputDocs);
    try {
      req.process(solr);
    } catch (SolrServerException e) {
      throw new IOException(e);
    }
    inputDocs.clear();
  }

  public void close() throws IOException {
    if (!inputDocs.isEmpty())
      update();
    try {
      solr.commit();
    } catch (SolrServerException e) {
    }
  }
}