package com.aliyun.odps.mma.server.action;

import com.aliyun.odps.mma.server.task.Task;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Base64;
import java.util.List;

public class HiveToOdpsTableHashVerificationAction extends DefaultAction {
    private final Logger LOG = LogManager.getLogger(HiveToOdpsTableHashVerificationAction.class);

    public HiveToOdpsTableHashVerificationAction(String id, Task task, ActionExecutionContext ctx) {
        super(id, task, ctx);
    }

    @Override
    void handleResult(Object result) {

    }

    @Override
    public Object call() throws Exception {
        Object hiveSqlResult = actionExecutionContext.getData("hive_table_hash");
        Object odpsSqlResult = actionExecutionContext.getData("odps_table_hash");

        VerificationResult hiveResult = VerificationResult.fromSqlResult((List<List<?>>) hiveSqlResult);
        VerificationResult odpsResult = VerificationResult.fromSqlResult((List<List<?>>) odpsSqlResult);

        LOG.info( "hive record count = {}, mc record count = {}", hiveResult.recordCount, odpsResult.recordCount);

        if (hiveResult.recordCount != odpsResult.recordCount) {
            throw  new RuntimeException(
                    String.format(
                            "Verification failed, hive record count=%d, odps record count=%d",
                            hiveResult.recordCount,
                            odpsResult.recordCount
                    )
            );
        }

        String hiveHash = hiveResult.tableHash;
        String odpsHash = odpsResult.tableHash;

        LOG.info( "hive hash = {}, mc hash = {}", hiveResult.tableHash, odpsResult.tableHash);

        if (! hiveHash.equals(odpsHash)) {
            throw  new RuntimeException(
                    String.format("Verification failed, hive hash=%s, odps hash=%s", hiveHash, odpsHash)
            );
        }

        return null;
    }

    @Override
    public String getName() {
        return "Final verification";
    }

    @Override
    public Object getResult() {
        return null;
    }

    static class VerificationResult {
        public long recordCount = 0;
        public String tableHash;

        public static VerificationResult fromSqlResult(List<List<?>> sqlResult) {
            VerificationResult result = new VerificationResult();

            byte[] tableHashBytes = new byte[32];
            byte[] xorHashBytes = new byte[16];
            byte[] addHashBytes = new byte[16];

            Base64.Decoder decoder = Base64.getDecoder();

            for (List<?> record: sqlResult) {
                Object count = record.get(0);
                String hash = (String) record.get(1);

                if (count instanceof String) {
                    result.recordCount += Long.parseLong((String) count);
                } else {
                    result.recordCount += (Long) count;
                }

                byte[] hashBytes = decoder.decode(hash);

                for (int i = 0, n = 16; i < n; i ++) {
                    xorHashBytes[i] = (byte)(xorHashBytes[i] ^ hashBytes[i]);
                    addHashBytes[i] = (byte)(addHashBytes[i] + hashBytes[i + 16]);
                }
            }

            System.arraycopy(xorHashBytes, 0, tableHashBytes, 0, 16);
            System.arraycopy(addHashBytes, 0, tableHashBytes, 16, 16);

            Base64.Encoder encoder = Base64.getEncoder();
            result.tableHash = encoder.encodeToString(tableHashBytes);

            return result;
        }
    }
}
