package com.aliyun.odps.compiler;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.compiler.log.LogItem;
import com.aliyun.odps.mma.sql.Subst;
import com.aliyun.odps.mma.sql.SubstGroup;


public abstract class SubstScanner extends TreeScanner<Subst, LogItem> {
  public Subst subst(LogItem logItem, SyntaxTreeNode ast) {
    return scan(ast, logItem);
  }

  @Override
  public Subst reduce(Subst r1, Subst r2) {
    if (r1 != null && r2 != null) {
      List<Subst> subst = new ArrayList<>();
      if (r1 instanceof SubstGroup) {
        subst.addAll(((SubstGroup) r1).getSubstList());
      } else {
        subst.add(r1);
      }
      if (r2 instanceof SubstGroup) {
        subst.addAll(((SubstGroup) r2).getSubstList());
      } else {
        subst.add(r2);
      }
      return new SubstGroup(subst);
    }
    return r1 == null ? r2 : r1;

  }
}