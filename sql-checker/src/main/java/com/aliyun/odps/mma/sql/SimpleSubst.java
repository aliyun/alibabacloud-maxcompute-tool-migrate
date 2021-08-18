package com.aliyun.odps.mma.sql;

public class SimpleSubst implements Subst {

  private final int start;
  private final int end;
  private final String subst;

  public SimpleSubst(int start, int end, String subst) {
    this.start = start;
    this.end = end;
    this.subst = subst;
  }

  @Override
  public int start() {
    return start;
  }

  @Override
  public int end() {
    return end;
  }

  @Override
  public String subst(String sql) {
    return sql.substring(0, start) + subst + sql.substring(end);
  }
}
