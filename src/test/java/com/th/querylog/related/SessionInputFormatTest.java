package com.th.querylog.related;

import static org.junit.Assert.*;

import org.junit.Test;

public class SessionInputFormatTest {

  @Test
  public void testGetAnonId() {
    String queryLine = "142\trentdirect.com\t2006-03-01 07:17:12";
    String anonId = SessionInputFormat.getAnonId(queryLine);
    assertEquals("142", anonId);
  }

}
