package com.th.querylog.extract;

import com.th.querylog.model.Click;
import com.th.querylog.model.Query;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AOLExtractor implements IExtractor {
  private static final SimpleDateFormat sdf = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss");

  public Query extract(String line) {
    String[] cols = line.split("\t");
    Query query = null;

    if ((cols.length == 3) || (cols.length == 5)) {
      if (cols.length == 5) {
        Click click = new Click();
        int itemRank = 0;
        try {
          itemRank = Integer.parseInt(cols[3]);
        } catch (Exception e) {
        }
        click.setItemRank(itemRank);

        String clickUrl = cols[4];
        click.setClickUrl(clickUrl);

        query = click;
      }

      if (cols.length == 3)
        query = new Query();

      long anonId = 0L;
      try {
        anonId = Long.parseLong(cols[0]);
      } catch (Exception e) {
      }
      query.setAnonId(anonId);
      query.setQuery(cols[1]);
      Date queryTime = null;
      try {
        queryTime = sdf.parse(cols[2]);
      } catch (ParseException e) {
      }
      query.setQueryTime(queryTime);
    }

    return query;
  }
}