package com.aliyun.odps.mma.validation;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.io.IOException;
import java.util.Base64;

// 这个udtf暂时没有用到
@Resolve("bigint,string->bigint,string")
public class OdpsPartitionedHashSumUDTF extends UDTF  {
    private byte[] tableXorHash = new byte[16];
    private byte[] tableAddHash = new byte[16];
    private long count;

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {
        super.setup(ctx);
    }

    @Override
    public void process(Object[] objects) throws UDFException, IOException {
        Base64.Decoder decoder = Base64.getDecoder();
        count += (long)objects[0];
        String partitionedHash = (String) objects[1];

        byte[] hashBytes = decoder.decode(partitionedHash);

        for (int i = 0, n = 16; i < n; i ++) {
            tableXorHash[i] = (byte)(tableXorHash[i] ^ hashBytes[i]);
            tableAddHash[i] = (byte)(tableAddHash[i] + hashBytes[i + 16]);
        }

    }

    @Override
    public void close() throws UDFException {
        Base64.Encoder encoder = Base64.getEncoder();
        byte[] tableHash = new byte[32];

        System.arraycopy(tableXorHash, 0, tableHash, 0, 16);
        System.arraycopy(tableAddHash, 0, tableHash, 16, 16);
        forward(count, encoder.encodeToString(tableHash));
        super.close();
    }
}
