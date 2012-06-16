package com.th.querylog.extract;

import com.th.querylog.model.Click;
import com.th.querylog.model.Query;
import org.junit.Assert;
import org.junit.Test;

public class AOLExtractorTest {
  @Test
  public void testExtract() {
    String queryLine = "142\trentdirect.com\t2006-03-01 07:17:12";
    String clickLine = "142\twestchester.gov\t2006-03-20 03:55:57\t1\thttp://www.westchestergov.com";
    IExtractor extractor = new AOLExtractor();
    Query query = extractor.extract(queryLine);

    Assert.assertNotNull(query);
    Assert.assertEquals(query.getClass(), Query.class);

    query = extractor.extract(clickLine);

    Assert.assertNotNull(query);
    Assert.assertEquals(query.getClass(), Click.class);
  }
}