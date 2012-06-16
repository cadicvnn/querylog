package com.th.querylog.model;

import java.util.Date;

public class Query {
  private long anonId;
  private String query;
  private Date queryTime;

  public long getAnonId() {
    return this.anonId;
  }

  public void setAnonId(long anonId) {
    this.anonId = anonId;
  }

  public String getQuery() {
    return this.query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public Date getQueryTime() {
    return this.queryTime;
  }

  public void setQueryTime(Date queryTime) {
    this.queryTime = queryTime;
  }
}