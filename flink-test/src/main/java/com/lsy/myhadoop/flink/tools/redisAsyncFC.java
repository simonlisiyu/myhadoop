package com.lsy.myhadoop.flink.tools;

import com.lsy.myhadoop.flink.domain.alter_change;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.Collections;

public class redisAsyncFC extends RichAsyncFunction<alter_change,alter_change> {
    private transient RedisClient redisClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RedisOptions config = new RedisOptions();
        config.setPort(6379);
        config.setHost("10.128.1.63");

        VertxOptions vo = new VertxOptions();
        vo.setWorkerPoolSize(200);

        Vertx vx = Vertx.vertx(vo);

        redisClient = RedisClient.create(vx, config);

    }

    @Override
    public void close() throws Exception {
        super.close();

        if (redisClient!=null)
            redisClient.close(null);
    }

    @Override
    public void timeout(alter_change input, ResultFuture<alter_change> resultFuture) throws Exception {
        if (input.getLine_no()!=null&&input.getNow_stop_order()!=null&&input.getCard_no()!=null) {

            ArrayList<alter_change> alter_changes = new ArrayList<>();
        MyHashMap hp = input.getHp();
        MyRandom random = new MyRandom();
        int i1 = random.nextInt(7);
        Integer rd = null;
        rd = i1+input.getNow_stop_order();
        if (rd>=input.getEnd_station_order()){
            int i = input.getEnd_station_order() - input.getNow_stop_order();
            rd = random.nextInt(i + 1) + input.getNow_stop_order();
        }
        hp.put(rd, -1);
        input.setHp(hp);
        alter_changes.add(input);
        resultFuture.complete(alter_changes);
    }else
        resultFuture.complete(Collections.singleton(input));
    }

    @Override
    public void asyncInvoke(alter_change alter_change, ResultFuture<alter_change> resultFuture) throws Exception {
        ArrayList<alter_change> alter_changes = new ArrayList<>();
        if (alter_change.getLine_no()!=null&&alter_change.getNow_stop_order()!=null&&alter_change.getCard_no()!=null) {


            MyHashMap hp = alter_change.getHp();

            String upordown = null;
            if (alter_change.getBus_UpOrDown().equals("上行")) upordown = "0";
            else upordown = "1";

            String line_n_upordown = alter_change.getLine_no() + "," + upordown;
            String card_id = alter_change.getCard_no();
            Integer now_stop_order = alter_change.getNow_stop_order();
            String now_filed = card_id + now_stop_order;
            String last_filed = card_id + (alter_change.getNow_stop_order() - 1);
            String next_filed = card_id + (alter_change.getNow_stop_order() + 1);


//        先查，现在站点是否有匹配
            redisClient.hget(line_n_upordown, now_filed, getres1 -> {
//           若现在站点匹配。则返回下车点。与hp添加下车位置
                if (getres1.succeeded() && Integer.valueOf(getres1.result().split(",")[0]) > now_stop_order) {
                    alter_change.setOff_stop_order(Integer.valueOf(getres1.result().split(",")[0]));
                    hp.put(Integer.valueOf(getres1.result().split(",")[0]), -1);
                    alter_change.setHp(hp);
                    alter_changes.add(alter_change);
                    resultFuture.complete(alter_changes);
                }
                else if (getres1.failed()){
                    resultFuture.complete(null);
                }
                else {
                    MyRandom random = new MyRandom();
                    int i1 = random.nextInt(7);
                    Integer rd = null;
                    rd = i1 + alter_change.getNow_stop_order();
                    if (rd >= alter_change.getEnd_station_order()) {
                        int i = alter_change.getEnd_station_order() - alter_change.getNow_stop_order();
                        rd = random.nextInt(i + 1) + alter_change.getNow_stop_order();
                    }
                    hp.put(rd, -1);
                    alter_change.setHp(hp);
                    alter_changes.add(alter_change);
                    resultFuture.complete(alter_changes);
                }

//                else
////               若当前站点没有画像，则向上一个站点搜索是否存在画像
//                {
//                    redisClient.hget(line_n_upordown, last_filed, getres2 -> {
//                        if (getres2.succeeded() && Integer.valueOf(getres2.result().split(",")[0]) > now_stop_order) {
//                            alter_change.setOff_stop_order(Integer.valueOf(getres2.result().split(",")[0]));
//                            hp.put(Integer.valueOf(getres2.result().split(",")[0]), -1);
//                            alter_change.setHp(hp);
//                            alter_changes.add(alter_change);
//                            resultFuture.complete(alter_changes);
//                        }
////                   上一个站点未关联到用户画像，则取下一个站点是否有画像
//                        else {
//                            redisClient.hget(line_n_upordown, next_filed, getres3 -> {
//                                if (getres3.succeeded() && Integer.valueOf(getres3.result().split(",")[0]) > now_stop_order) {
//                                    alter_change.setOff_stop_order(Integer.valueOf(getres3.result().split(",")[0]));
//                                    hp.put(Integer.valueOf(getres3.result().split(",")[0]), -1);
//                                    alter_change.setHp(hp);
//                                    alter_changes.add(alter_change);
//                                    resultFuture.complete(alter_changes);
//                                }
////                          前后现站点都并未关联上用户画像，则取随机下车点（本应取线路热点站点）
//                                else {
//                                    MyRandom random = new MyRandom();
//                                    int i1 = random.nextInt(7);
//                                    Integer rd = null;
//                                    rd = i1 + alter_change.getNow_stop_order();
//                                    if (rd >= alter_change.getEnd_station_order()) {
//                                        int i = alter_change.getEnd_station_order() - alter_change.getNow_stop_order();
//                                        rd = random.nextInt(i + 1) + alter_change.getNow_stop_order();
//                                    }
//                                    hp.put(rd, -1);
//                                    alter_change.setHp(hp);
//                                    alter_changes.add(alter_change);
//                                    resultFuture.complete(alter_changes);
//                                }
//                            });
//                        }
//                    });
//                }
            });
        }
        else {
            alter_changes.add(alter_change);
            resultFuture.complete(alter_changes);
        }

    }
}
