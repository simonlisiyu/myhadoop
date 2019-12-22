package com.lsy.myhadoop.es.client;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


/**
 * Created by lisiyu on 2017/1/13.
 */
public class ElasticSearchBulkProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchBulkProcessor.class);

    private static BulkProcessor bulkProcessor = null;

    public synchronized static void initElasticSearchBulkProcessor(Client client, int maxCount, int maxSizeMb, int maxTimeIntervalSec,
                                                                   int maxConcurrentRequest, int rejectExceptionRetryWaitMillis, int rejectExceptionRetryTimes){
        LOG.debug("enter ElasticSearchBulkProcessor.initElasticSearchBulkProcessor()");
        if(bulkProcessor == null){
            bulkProcessor = BulkProcessor.builder(
                    client,
                    new BulkProcessor.Listener() {
                        @Override
                        public void beforeBulk(long executionId,
                                               BulkRequest request) {  }

                        @Override
                        public void afterBulk(long executionId,
                                              BulkRequest request,
                                              BulkResponse response) {  }

                        @Override
                        public void afterBulk(long executionId,
                                              BulkRequest request,
                                              Throwable failure) {  }
                    })
                    .setBulkActions(maxCount)
                    .setBulkSize(new ByteSizeValue(maxSizeMb, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(maxTimeIntervalSec))
                    .setConcurrentRequests(maxConcurrentRequest)
                    .setBackoffPolicy(
                            BackoffPolicy.exponentialBackoff(
                                    TimeValue.timeValueMillis(rejectExceptionRetryWaitMillis),
                                    rejectExceptionRetryTimes))
                    .build();
            LOG.info("ES_BULK_PROCESSOR_MAX_BULK_COUNT="+maxCount +
                    ", ES_BULK_PROCESSOR_MAX_BULK_SIZE="+maxSizeMb +
                    ", ES_BULK_PROCESSOR_MAX_COMMIT_INTERVAL="+maxTimeIntervalSec +
                    ", ES_BULK_PROCESSOR_MAX_CONCURRENT_REQUEST="+maxConcurrentRequest +
                    ", ES_BULK_PROCESSOR_REJECT_EXCEPTION_RETRY_WAIT="+rejectExceptionRetryWaitMillis +
                    ", ES_BULK_PROCESSOR_REJECT_EXCEPTION_RETRY_TIMES="+rejectExceptionRetryTimes);
        }
        LOG.debug("success ElasticSearchBulkProcessor.initElasticSearchBulkProcessor()");
    }

    public synchronized static void closeElasticSearchBulkProcessor(Client client){
        LOG.debug("enter ElasticSearchBulkProcessor.closeElasticSearchBulkProcessor()");
        if(bulkProcessor != null){
            bulkProcessor.close();
            bulkProcessor = null;
            client.close();
        }
        LOG.debug("success ElasticSearchBulkProcessor.closeElasticSearchBulkProcessor()");
    }

    /**
     * 加入索引请求到缓冲池
     *
     //     * @param indexRequest
     * @param jsonObject
     */
    public static void addIndexRequestToBulkProcessor(String index, String type, JSONObject jsonObject) {
        LOG.debug("enter ElasticSearchBulkProcessor.addIndexRequestToBulkProcessor()" +
                ", index="+index+
                ", type="+type+
                ", jsonObject="+jsonObject);

        String rowId = jsonObject.getString("id");
        jsonObject.remove("id");
        String key = null;
        String value = null;
        HashMap json = new HashMap();
        for(Object object : jsonObject.keySet()){
            key = object.toString();

            value = jsonObject.getString(key);
            if(key.contains(".")){
                key = key.replace(".","");
            }
            json.put(key, value);
        }
        UpdateRequest updateRequest = new UpdateRequest(index, type, rowId).doc(json);
        try {
            // 更新数据
            bulkProcessor.add(updateRequest.docAsUpsert(true));
        } catch (Exception ex) {
            LOG.error("Fail to bulkProcessor.add(updateRequest.docAsUpsert(true)). Exception="+ ExceptionUtils.getStackTrace(ex));
        }
        LOG.info("index="+index+
                ", type="+type+
                ", jsonObject="+jsonObject);
        LOG.debug("success ElasticSearchBulkProcessor.addIndexRequestToBulkProcessor()");
    }
}
