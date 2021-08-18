package com.aliyun.odps.mma.sql;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class SubstGroup implements Subst {

  private final List<Subst> substList;

  public SubstGroup(List<Subst> substList) {
    this.substList = substList.stream()
                              .sorted(Comparator.comparing(Subst::start).reversed()).collect(Collectors.toList());
  }

  public List<Subst> getSubstList() {
    return substList;
  }

  @Override
  public int start() {
    return substList.stream().map(Subst::start).min(Comparator.comparing(a->a)).orElse(0);
  }

  @Override
  public int end() {
    return substList.stream().map(Subst::end).max(Comparator.comparing(a->a)).orElse(0);
  }

  @Override
  public String subst(String sql) {
    for (Subst subst : substList) {
      sql = subst.subst(sql);
    }
    return sql;
  }
}
