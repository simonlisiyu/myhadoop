import com.lsy.myhadoop.es.client.ElasticSearchTransportClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

/**
 * Created by lisiyu on 2017/1/22.
 */
public class EsOutputTimeRange {
    public static void main(String[] args) {
        String index = "mjdos-new";
        String type = "log";
        String fileName = "es1";
        Date to = new Date();
        Date from = new Date();
        if (args.length == 5) {
            index = args[0];
            type = args[1];
            fileName = args[2];
            to = new Date(Long.parseLong(args[3]));
            from = new Date(Long.parseLong(args[4]));
            System.out.println("index="+index);
            System.out.println("type="+type);
            System.out.println("fileName="+fileName);
            System.out.println("to="+to);
            System.out.println("from="+from);
        }

        QueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.rangeQuery("datetime").to(to).from(from));

        Client client = ElasticSearchTransportClient.client;
        SearchResponse response = client.prepareSearch(index).setTypes(type)
                .setQuery(query).setSize(10000).setScroll(new TimeValue(600000))
                .setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();//setSearchType(SearchType.Scan) 告诉ES不需要排序只要结果返回即可 setScroll(new TimeValue(600000)) 设置滚动的时间
        String scrollid = response.getScrollId();
        try {
            //把导出的结果以JSON的格式写到文件里
            BufferedWriter out = new BufferedWriter(new FileWriter(fileName, true));

            //每次返回数据10000条。一直循环查询直到所有的数据都查询出来
            while (true) {
                SearchResponse response2 = client.prepareSearchScroll(scrollid).setScroll(new TimeValue(1000000))
                        .execute().actionGet();
                SearchHits searchHit = response2.getHits();
                //再次查询不到数据时跳出循环
                if (searchHit.getHits().length == 0) {
                    break;
                }
                System.out.println("search count :" + searchHit.getHits().length);
                for (int i = 0; i < searchHit.getHits().length; i++) {
                    String json = searchHit.getHits()[i].getSourceAsString();
                    out.write(json);
                    out.write("\r\n");
                }
            }
            System.out.println("search finished.");
            out.close();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}
