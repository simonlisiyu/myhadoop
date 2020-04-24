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
 * Created by lisiyu on 2020/4/7.
 */
public class EsImport2Test {
    public static String DEPT_DATA = "5dec0ab13c,松下秘书科,平原市公安局交通警察支队松下区大队秘书科,翠缕,85082518,85082518,中环路568号\n" +
            "a1ecee6272,玉琼中队,平原市公安局交通警察支队市南区大队玉琼路中队,翠墨,85082520,85082520,中环路568号\n" +
            "0c0080154e,事故处,平原市公安局交通警察支队交通肇事处理处,翠云,85082282,88551194,二环东路6897号\n" +
            "0e91f0b535,原办事处执法站,平原市公安局交通警察支队松下区大队情侣路中队原办事处执法站,大姐,85103161,85103161,中环路568号\n" +
            "a5b4f4a0bd,塘子巷检查站,塘子巷检查站,大了,85088269,85088269,中环路568号\n" +
            "91f7c0719f,二大队棋盘山中队,平原市公安局交通警察支队芜湖区大队棋盘山中队,戴良,85082200,85082300,中环路568号\n" +
            "c4314d96a4,市南大队三中队,平原市公安局交通警察支队市南区大队三中队,戴权,85082520,85082520,中环路568号\n" +
            "05886f32af,天山大队四中队,平原市公安局交通警察支队天山区大队四中队,靛儿,85082540,85082540,中环路568号\n" +
            "8bc650267f,天山大队一中队,平原市公安局交通警察支队天山区大队一中队,定儿,85702540,85702540,中环路568号\n" +
            "bc54727428,聚源五峰中队,平原市公安局交通警察支队聚源区大队五峰中队,豆官,87211901,87211901,中环路568号";


    public static void main(String[] args) throws UnknownHostException {
        String index = "dws_kg_vertex_dept";
        String type = "f";

        System.out.println("index="+index);
        System.out.println("type="+type);

        Client client = ElasticSearchTransportClient.getInstance("10.179.117.160");
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
                .setBulkActions(100)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(2))
                .setConcurrentRequests(3)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(6000), 3))
                .build();

        long ts = System.currentTimeMillis();
        //读取刚才导出的ES数据
        String[] lineSplit = DEPT_DATA.split("\n");
        for(String line : lineSplit){
            String[] splits = line.split(",");
            JSONObject dataJson = new JSONObject();
            dataJson.put("obj_kg_label", "pol_dept");
            dataJson.put("obj_id", splits[0]);
            dataJson.put("obj_name", splits[1]);
            dataJson.put("obj_create_at", ts);
            dataJson.put("obj_create_by", "admin");
            dataJson.put("obj_update_at", ts);
            dataJson.put("obj_update_by", "admin");
            dataJson.put("obj_is_deleted", false);
            dataJson.put("dept_id", splits[0]);
            dataJson.put("dept_name", splits[1]);
            dataJson.put("dept_fullname", splits[2]);
            dataJson.put("dept_contact", splits[3]);
            dataJson.put("work_phone", splits[4]);
            dataJson.put("work_fax", splits[5]);
            dataJson.put("work_address", splits[6]);

            String id = splits[0];
            System.out.println(id);
            System.out.println(dataJson);
            bulkProcessor.add(new IndexRequest(index, type, id).source(dataJson));
            UpdateRequest updateRequest = new UpdateRequest(index, type, id).doc(dataJson);
            bulkProcessor.add(updateRequest.docAsUpsert(true));
        }

        System.out.println("import finished.");
    }


}
