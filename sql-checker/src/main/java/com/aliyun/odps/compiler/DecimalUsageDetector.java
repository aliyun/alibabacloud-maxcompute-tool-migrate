package com.aliyun.odps.compiler;

import com.aliyun.odps.compiler.type.DecimalType;

public class DecimalUsageDetector extends CaseDetector {
  private boolean used = false;

  @Override
  protected Void scan(SyntaxTreeNode t, Void aVoid) {
    if (used) {
      return null;
    }

    if (t != null && TreeExt.type(t) instanceof DecimalType) {
      emit("decimal.usage", t);
      used = true;
      return null;
    }

    return super.scan(t, aVoid);
  }
}
