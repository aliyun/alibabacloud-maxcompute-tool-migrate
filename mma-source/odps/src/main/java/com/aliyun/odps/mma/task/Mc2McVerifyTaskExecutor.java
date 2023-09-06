package com.aliyun.odps.mma.task;

import com.aliyun.odps.Instance;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.util.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class Mc2McVerifyTaskExecutor extends TaskExecutor {
    Logger logger = LoggerFactory.getLogger(Mc2McVerifyTaskExecutor.class);

    OdpsAction sourceOdpsAction;

    Instance odpsSrcIns;
    Instance odpsDstIns;

    public Mc2McVerifyTaskExecutor() {
        super();
    }

    @Override
    public TaskType taskType() {
        return TaskType.MC2MC_VERIFY;
    }

    @Override
    protected void setUp() {
        sourceOdpsAction = OdpsAction.getSourceOdpsAction((OdpsConfig) sourceConfig, task);
    }

    @Override
    protected void _setUpSchema() throws Exception {}

    @Override
    protected void _dataTruncate() throws Exception {}

    @Override
    protected void _dataTrans() throws Exception {}

    @Override
    protected void _verifyData() throws Exception {
        CompletableFuture<Long> destCountFuture = odpsAction.selectCount(
                task.getOdpsTableFullName(),
                (instance -> this.odpsDstIns = instance)
        );
        CompletableFuture<Long> srcCountFuture = sourceOdpsAction.selectCount(
                task.getTableFullName(),
                (instance -> this.odpsSrcIns = instance)
        );
        srcCountFuture.join();

        VerificationAction.countResultCompare("source odps", srcCountFuture.get(),
                                        "dest odps", destCountFuture.get(),
                                        task);
    }

    @Override
    public void killSelf() {
        super.killSelf();

        OdpsUtils.stop(odpsSrcIns);
        odpsSrcIns = null;

        OdpsUtils.stop(odpsDstIns);
        odpsDstIns = null;
    }
}
