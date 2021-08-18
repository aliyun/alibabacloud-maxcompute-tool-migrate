package com.aliyun.odps.compiler;

import com.aliyun.odps.compiler.function.ResolvedFunctionSignature;

public class UsedFunctionDetector extends CaseDetector {

  @Override
  public Void visit(FunctionCall t, Void aVoid) {
    ResolvedFunctionSignature sig = TreeExt.sig(t);
    if (sig != null && sig.getTarget() != null && !sig.getTarget().isBuiltIn()) {
      emit("used.function", t);
    }
    return super.visit(t, null);
  }
}
