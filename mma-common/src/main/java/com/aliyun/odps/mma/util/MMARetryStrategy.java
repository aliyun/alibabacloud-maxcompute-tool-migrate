package com.aliyun.odps.mma.util;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.util.RetryExceedLimitException;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.rest.RestClient;

public class MMARetryStrategy extends RetryStrategy {

  private TaskProxy task;
  Logger log = LoggerFactory.getLogger(MMARetryStrategy.class);

  public MMARetryStrategy(TaskProxy task, int limit) {
    super(limit, 1, BackoffStrategy.CONSTANT_BACKOFF);
    this.task = task;
  }

  @Override
  public void onFailure(String resource, Exception err, RestClient.RetryLogger logger)
      throws RetryExceedLimitException, InterruptedException {
    if (attempts++ >= limit) {
      attempts = 0;
      throw new RetryExceedLimitException(attempts, err);
    }

    String msg = "RESOURCE: " + resource + "   ERROR MESSAGE: " + err.getMessage();

    if (err instanceof OdpsException) {
      if (((OdpsException) err).getRequestId() != null) {
        msg += " requestId: " + ((OdpsException) err).getRequestId();
      }
    }

    Random random = new Random();
    int sleep = random.nextInt(7) + 3;
    Thread.sleep(sleep * 1000);


    if (task != null) {
      task.log(
          "odps request failed, attempts: " + attempts + ", will retry in " + sleep + " seconds",
          msg);
    }
    log.info("odps request failed, attempts: " + attempts + ", will retry in " + sleep + " seconds" + msg);
  }
}
