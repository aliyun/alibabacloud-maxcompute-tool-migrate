package com.aliyun.odps.compiler;

public class IssueDetector {

  public static com.aliyun.odps.compiler.CaseDetector[] getDetectors() {
    return new CaseDetector[]{
        new UsedFunctionDetector(),
        new DecimalUsageDetector()
    };
  }
}
