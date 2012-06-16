package com.th.querylog.model;

public class Click extends Query {
  private int itemRank;
  private String clickUrl;

  public int getItemRank() {
    return this.itemRank;
  }

  public void setItemRank(int itemRank) {
    this.itemRank = itemRank;
  }

  public String getClickUrl() {
    return this.clickUrl;
  }

  public void setClickUrl(String clickUrl) {
    this.clickUrl = clickUrl;
  }
}