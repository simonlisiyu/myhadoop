import com.alibaba.fastjson.JSONObject;
import com.lsy.myhadoop.es.client.ElasticSearchTransportClient;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Created by lisiyu on 2017/1/22.
 */
public class EsImport2 {
    public static void main(String[] args) throws UnknownHostException {
        String index = "people_3_201_1_test";
        String type = "people_3_201_1";
        String fileName = "estest1";
        if (args.length == 3) {
            index = args[0];
            type = args[1];
            fileName = args[2];
        }
        System.out.println("index="+index);
        System.out.println("type="+type);
        System.out.println("fileName="+fileName);

        Client client = ElasticSearchTransportClient.getInstance("10.179.40.207");
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long l, BulkRequest bulkRequest) {
//                        try {
//                            createMapping1("test1",  "data", set);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

                    }

                    @Override
                    public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {

                    }

                })
                .setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(6))
                .setConcurrentRequests(3)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(6000), 3))
                .build();

        long ts = System.currentTimeMillis();
        try {
            //读取刚才导出的ES数据
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String data = null;
            int count = 0;

            while ((data = br.readLine()) != null) {
                String[] splits = data.split(",");
                JSONObject json = new JSONObject();
                if (splits.length>0) {
                    json.put("XM", splits[1]);
                    json.put("XB", splits[2]);
                    json.put("_updatetime", ts);
                    json.put("SFZHM", splits[0]);
                }
                String id = IdGen.uuid()+(++count);
                System.out.println(id);
                System.out.println(json);
                bulkProcessor.add(new IndexRequest(index, type, id).source(json));
                UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(json);
                bulkProcessor.add(updateRequest.docAsUpsert(true));
            }

            System.out.println("import finished.");
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
