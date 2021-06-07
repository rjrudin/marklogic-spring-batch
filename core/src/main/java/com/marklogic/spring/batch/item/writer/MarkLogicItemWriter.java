package com.marklogic.spring.batch.item.writer;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatchListener;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.datamovement.WriteFailureListener;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.document.ServerTransform;
import com.marklogic.client.io.Format;
import com.marklogic.spring.batch.item.writer.support.DefaultUriTransformer;
import com.marklogic.spring.batch.item.writer.support.UriTransformer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

/**
 * The MarkLogicItemWriter is an ItemWriter used to write any type of document to MarkLogic. It expects a list of
 * <a href="http://docs.marklogic.com/javadoc/client/com/marklogic/client/document/DocumentWriteOperation.html">DocumentWriteOperation</a>
 * instances, each of which encapsulates a write operation to MarkLogic.
 * <p>
 * A UriTransformer can be optionally set to transform the URI of each incoming DocumentWriteOperation.
 */
public class MarkLogicItemWriter implements ItemWriter<DocumentWriteOperation>, ItemStream {

    protected UriTransformer uriTransformer;
    protected DatabaseClient client;
    protected DataMovementManager dataMovementManager;
    private WriteBatcher batcher;
    private int batchSize = 100;
    private int threadCount = 4;
    private ServerTransform serverTransform;
    private boolean isWriteAsync = true;
    private WriteBatchListener writeBatchlistener = null;
    private WriteFailureListener writeFailureListener = null;

    public MarkLogicItemWriter(DatabaseClient client) {
        this.client = client;
        uriTransformer = new DefaultUriTransformer();
    }

    public MarkLogicItemWriter(DatabaseClient client, ServerTransform serverTransform) {
        this(client);
        this.serverTransform = serverTransform;
    }

    @Override
    public void write(List<? extends DocumentWriteOperation> items) throws Exception {
        for (DocumentWriteOperation item : items) {
            batcher.add(uriTransformer.transform(item.getUri()), item.getMetadata(), item.getContent());
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        dataMovementManager = client.newDataMovementManager();
        batcher = dataMovementManager.newWriteBatcher();
        batcher
                .withBatchSize(getBatchSize())
                .withThreadCount(getThreadCount());
        if (serverTransform != null) {
            batcher.withTransform(serverTransform);
        }
        if (this.writeBatchlistener != null) {
                batcher.onBatchSuccess(this.writeBatchlistener);
        }
        if (this.writeFailureListener != null) {
                batcher.onBatchFailure(writeFailureListener);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        batcher.flushAndWait();
        dataMovementManager.release();
    }

    public void setServerTransform(ServerTransform serverTransform) {
        this.serverTransform = serverTransform;
    }

    public void setUriTransformer(UriTransformer uriTransformer) {
        this.uriTransformer = uriTransformer;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public boolean isWriteAsync() {
        return isWriteAsync;
    }

    public void setWriteAsync(boolean writeAsync) {
        isWriteAsync = writeAsync;
    }

    public void setWriteFailureListener(WriteFailureListener listener) {
        this.writeFailureListener = listener;
    }
    
    public void setWriteBatchListener(WriteBatchListener listener) {
		this.writeBatchlistener = listener;
    }
    
}

