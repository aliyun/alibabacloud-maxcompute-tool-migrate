package com.aliyun.odps.mma.validation;

import com.aliyun.odps.Column;
import com.aliyun.odps.mma.validation.TableHasherInter;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class TableHasher implements TableHasherInter {
    protected MessageDigest md;
    protected byte[] tableXorHash = new byte[16];
    protected byte[] tableAddHash = new byte[16];
    protected BaseFieldAdapter adapter;
    protected List<FieldVector> fieldVectors = new ArrayList<>();
    private FieldVectorsComparator comparator = new FieldVectorsComparator();

    public TableHasher() {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ignored) {
            // unreachable!
        }

        for (int i = 0, n = tableXorHash.length; i < n; i ++) {
            tableXorHash[i] = 0;
            tableAddHash[i] = 0;
        }
    }

    @Override
    public void initFieldAdapter(List<Column> columns, List<Column> partitionColumns) {

    }

    public BaseFieldAdapter getAdapter() {
        return adapter;
    }

    public void setAdapter(BaseFieldAdapter adapter) {
        this.adapter = adapter;
    }

    public String getTableHash() {
        byte[] bytes = new byte[32];

        System.arraycopy(tableXorHash, 0, bytes, 0, 16);
        System.arraycopy(tableAddHash, 0, bytes, 16, 16);

        return Base64.getEncoder().encodeToString(bytes);
    }
     public void addColumnData(Object ...objects) {
        FieldVector fieldVector = adapter.convert(objects);
        fieldVectors.add(fieldVector);
    }

    public void update() {
        Collections.sort(fieldVectors, comparator);
        VectorSchemaRoot root = new VectorSchemaRoot(fieldVectors);

        ByteArrayOutputStream bytesStream = new ByteArrayOutputStream();
        WritableByteChannel channel = Channels.newChannel(bytesStream);
        WriteChannel writeChannel = new WriteChannel(channel);
        VectorUnloader unLoader = new VectorUnloader(root);
        ArrowRecordBatch recordBatch = unLoader.getRecordBatch();

        try  {
            MessageSerializer.serialize(writeChannel, recordBatch);
        } catch (IOException e) {
            // unreachable!
        }

        byte[] bytes = bytesStream.toByteArray();
        md.reset();
        md.update(bytes);
        byte[] digest = md.digest();
        updateTableHash(digest);

        try {
            bytesStream.close();
            writeChannel.close();
        } catch (IOException e) {
            // unreachable!
        }

        root.close();
        root.clear();
        fieldVectors.clear();
    }

    private void updateTableHash(byte[] recordHash) {
        for (int i = 0, n = recordHash.length; i < n; i ++) {
            tableXorHash[i] = (byte)(tableXorHash[i] ^ recordHash[i]);
            tableAddHash[i] = (byte)(tableAddHash[i] + recordHash[i]);
        }
    }
}

class FieldVectorsComparator implements Comparator<FieldVector> {
    @Override
    public int compare(FieldVector o1, FieldVector o2) {
        return o1.getField().getName().compareTo(o2.getField().getName());
    }
}
