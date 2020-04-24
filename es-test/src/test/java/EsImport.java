import com.lsy.myhadoop.es.client.ElasticSearchTransportClient;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Created by lisiyu on 2017/1/22.
 */
public class EsImport {
    public static void main(String[] args) throws UnknownHostException {
        String index = "mjdos";
        String type = "log";
        String fileName = "es";
        String ip = "127.0.0.1";
        if (args.length == 4) {
            index = args[0];
            type = args[1];
            fileName = args[2];
            ip = args[3];
        }

        Client client = ElasticSearchTransportClient.getInstance(ip);

        String updateTime = System.currentTimeMillis() + "";
        try {
            //读取刚才导出的ES数据
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String json = null;
            int count = 0;
            //开启批量插入
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            while (((json = br.readLine()) != null)) {
            	int splitFlag = json.indexOf(",");
            	String id = "";
            	String body = "";
            	if (splitFlag>0) {
            		id = json.substring(0, splitFlag);
            		body = json.substring(splitFlag+1)+","+updateTime;
            	}
            	System.out.println("splitFlag:"+splitFlag);
            	System.out.println(id);
            	System.out.println(body);
//                bulkRequest.add(client.prepareIndex(index, type).setSource(body));
                bulkRequest.add(new UpdateRequest(index, type, id).doc(body).docAsUpsert(true));
                //每一千条提交一次
                if (count% 1000==0) {
                    bulkRequest.execute().actionGet();
                    System.out.println("提交了：" + count);
                }
                count++;
            }
            bulkRequest.execute().actionGet();
            System.out.println("插入完毕");
            br.close();
            client.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


}
