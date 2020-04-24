import com.lsy.myhadoop.es.client.ElasticSearchTransportClient;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.net.UnknownHostException;

/**
 * Created by lisiyu on 2017/1/22.
 */
public class EsOutput2 {
    public static void main(String[] args) throws UnknownHostException {
        String index = "dws_kg_vertex_obj_agg";
        String type = "f";
        String ip = "10.179.117.160";

        Client client = ElasticSearchTransportClient.getInstance(ip);
        SearchRequestBuilder builder = client.prepareSearch(index);
        if (type != null) {
            builder.setTypes(type);
        }
        builder.setQuery(QueryBuilders.matchAllQuery());
        builder.setSize(10000);
        builder.setScroll(new TimeValue(6000));
        SearchResponse scrollResp = builder.execute().actionGet();

        while (true) {    //循环插入，直到所有结束
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                String json = hit.getSourceAsString();
                System.out.println("json="+json);
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(6000)).execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        client.close();

        System.out.println("search finished.");
    }


}
