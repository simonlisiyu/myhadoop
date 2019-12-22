import com.lsy.myhadoop.es.client.ElasticSearchTransportClient;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by lisiyu on 16/9/9.
 */
public class BulkRequestExample {

    public static void main(String[] args) throws IOException {
        // on startup
        Client client = ElasticSearchTransportClient.getInstance("10.179.40.207");

        String index = "police_1_101_1_bj_test";
        String type = "police_1_101_1_bj";

        String fileName = "dcp-base-elasticsearch/src/main/resources/test2";
        if (args.length == 1) {
            fileName = args[0];
        }
        String data = null;

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        BufferedReader br = new BufferedReader(new FileReader(fileName));

        long ts = System.currentTimeMillis();
        int count = 0;
        while (((data = br.readLine()) != null)) {
            String[] splits = data.split(",");
            if (splits.length>0) {
                bulkRequest.add(client.prepareIndex(index, type, IdGen.uuid())
                        .setSource(jsonBuilder()
                                .startObject()
                                .field("JYW",splits[0])
                                .field("RDYYFL",splits[1])
                                .field("JBR",splits[2])
                                .field("JAFS",splits[3])
                                .field("SGSS",splits[4])
                                .field("LWSGLX",splits[5])
                                .field("DZZB",splits[6])
                                .field("GLBM",splits[7])
                                .field("SSRS",splits[8])
                                .field("JAR1",splits[9])
                                .field("GLS",splits[10])
                                .field("ZJCCSS",splits[11])
                                .field("BADW",splits[12])
                                .field("JAR2",splits[13])
                                .field("ZBLX",splits[14])
                                .field("XZQH",splits[15])
                                .field("SPR",splits[16])
                                .field("LM",splits[17])
                                .field("DAH",splits[18])
                                .field("LH",splits[19])
                                .field("TQ",splits[20])
                                .field("SWSG",splits[21])
                                .field("JLLX",splits[22])
                                .field("JMNR",splits[23])
                                .field("SCSJD",splits[24])
                                .field("DLLX",splits[25])
                                .field("SPTHYJ",splits[26])
                                .field("DJBH",splits[27])
                                .field("ZRTJJG",splits[28])
                                .field("SGFSSJ",splits[29])
                                .field("SGXT",splits[30])
                                .field("YWSBH",splits[31])
                                .field("XC",splits[32])
                                .field("GLXZDJ",splits[33])
                                .field("WSBH",splits[34])
                                .field("JDWZ",splits[35])
                                .field("CLJSG",splits[36])
                                .field("DCSG",splits[37])
                                .field("SGRDYY",splits[38])
                                .field("XQ",splits[39])
                                .field("ZZRQ",splits[40])
                                .field("SJLY",splits[41])
                                .field("SGBH",splits[42])
                                .field("TJR1",splits[43])
                                .field("LBQK",splits[44])
                                .field("SSZD",splits[45])
                                .field("GXSJ",splits[46])
                                .field("SPRQ",splits[47])
                                .field("CCLRSJ",splits[48])
                                .field("TJSGBH",splits[49])
                                .field("MS",splits[50])
                                .field("SB",splits[51])
                                .field("SGDD",splits[52])
                                .field("TJFS",splits[53])
                                .field("PZFS",splits[54])
                                .field("_updatetime", ts)
                                .endObject()
                        )
                );
            }

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

    }

//    public static void main(String[] args) throws IOException {
//        // on startup
//        Client client = ElasticSearchTransportClient.getInstance("10.179.40.207");
//
//
//        BulkRequestBuilder bulkRequest = client.prepareBulk();
//
//        // either use client#prepare, or use Requests# to directly build index/delete requests
//        bulkRequest.add(client.prepareIndex("people_3_201_1_test", "people_3_201_1", "125ee3a905744db29becb4ab8d2f2c931")
//                .setSource(jsonBuilder()
//                        .startObject()
//                        .field("XM", "张义峰")
//                        .field("XB", "男")
//                        .field("_updatetime", 1560165073452)
//                        .field("SFZHM", "370104198007120330")
//                        .endObject()
//                )
//        );
//
////        bulkRequest.add(client.prepareIndex("people_3_201_1_test", "people_3_201_1", "2")
////                .setSource(jsonBuilder()
////                        .startObject()
////                        .field("XM", "方传伟")
////                        .field("XB", "nan")
////                        .field("_updatetime", System.currentTimeMillis())
////                        .field("SFZHM", "370983198805292814")
////                        .endObject()
////                )
////        );
//
//        BulkResponse bulkResponse = bulkRequest.get();
//        if (bulkResponse.hasFailures()) {
//            // process failures by iterating through each bulk response item
//        }
//    }



}
