package com.th.querylog.extract;

import com.th.querylog.model.Query;

public abstract interface IExtractor {
  public abstract Query extract(String line);
}