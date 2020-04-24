package com.lsy.myhadoop.flink;

import com.google.gson.Gson;
import com.lsy.myhadoop.flink.domain.alter_change;
import com.lsy.myhadoop.flink.domain.card_pojo;
import com.lsy.myhadoop.flink.tools.*;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class card_bus {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> ud_pay_card =
                env.addSource(UD_PAY_CARD.get_ud_pay_card()).setParallelism(6);
        SingleOutputStreamOperator<card_pojo> card =
                ud_pay_card.map(new MapFunction<String, card_pojo>() {
                    public card_pojo map(String s) throws Exception {
                        Gson gson = new Gson();
                        card_pojo card_pojo = gson.fromJson(s, card_pojo.class);
                        if (card_pojo.getLineNo()!=null){
                            card_pojo.setLineNo(card_pojo.getLineNo().replace("路", ""));
                        }
                        return card_pojo;
                    }
                });
//                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<card_pojo>() {
//                    private final long maxOutOfOrderness = 180000; // 180 seconds
//
//                    private long currentMaxTimestamp;
//                    @Nullable
//                    @Override
//                    public Watermark getCurrentWatermark() {
//                        return new Watermark(currentMaxTimestamp-maxOutOfOrderness);
//                    }
//
//                    @Override
//                    public long extractTimestamp(card_pojo card_pojo, long l) {
//                        String txnDate = card_pojo.getTxnDate();
//                        Long Txn_date = Long.valueOf(txnDate);
//                        currentMaxTimestamp=Math.max(Txn_date,currentMaxTimestamp);
//                        return Txn_date;
//                    }
//                });

        DataStreamSource<String> alter_source = env.addSource(Bus_source.get_alter_change()).setParallelism(3);
        SingleOutputStreamOperator<alter_change> map = alter_source.map(new MapFunction<String, alter_change>() {
            @Override
            public alter_change map(String s) throws Exception {
                Gson gson = new Gson();
                alter_change alter_change = gson.fromJson(s, alter_change.class);
                return alter_change;
            }
        });
        /*巴士状态数据cogroup刷卡数据得到巴士上车数据*/
        SingleOutputStreamOperator<alter_change> bus_goup_card =
                map.coGroup(card).where(new KeySelector<alter_change, String>() {
                    @Override
                    public String getKey(alter_change alter_change) throws Exception {
                        return alter_change.getLine_no()+","+alter_change.getBus_no();
                    }
                }).equalTo(new KeySelector<card_pojo, String>() {
                    @Override
                    public String getKey(card_pojo card_pojo) throws Exception {
                        return card_pojo.getLineNo() +","+ card_pojo.getBusNo();
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                        .allowedLateness(Time.seconds(8))
                        .apply(new CoGroupFunction<alter_change, card_pojo, alter_change>() {
                            @Override
                            public void coGroup(Iterable<alter_change> alter_changes, Iterable<card_pojo> card_pojos,
                                                Collector<alter_change> collector) throws Exception {
                                alter_changes.forEach(alter_change -> {
                                    card_pojos.forEach(card_pojo -> {
                                        alter_change.setCard_no(card_pojo.getCardNo());
                                        alter_change.setCount(1);
                                        collector.collect(alter_change);
                                    });
                                    if (!card_pojos.iterator().hasNext()) {
                                        alter_change.setCard_no(null);
                                        alter_change.setCount(0);
                                        collector.collect(alter_change);
                                    }
                                });
                            }
                        })
                        /*初始化自定义hashmap*/
                        .map(new MapFunction<alter_change, alter_change>() {
                            @Override
                            public alter_change map(alter_change alter_change) throws Exception {
                                MyHashMap myHashMap = new MyHashMap();
                                alter_change.setHp(myHashMap);
                                return alter_change;
                            }
                        });

//        谁上车，车辆实时状态，车辆实时状态done
        /*关联用户画像维表得到下车站点以及人数两个指标*/
        SingleOutputStreamOperator<alter_change> alter_change_output = AsyncDataStream.orderedWait(bus_goup_card,


//                new RichAsyncFunction<alter_change, alter_change>() {
//            Jedis jedis = null;
//            GetRedisConnect getRedisConnect = null;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                getRedisConnect = new GetRedisConnect();
//                jedis = getRedisConnect.getRedisPoolConnect();
//            }
//
//
//            @Override
//            public void asyncInvoke(alter_change alter_change, ResultFuture<alter_change> resultFuture) throws Exception {
//                ArrayList<pojo.alter_change> alter_changes = new ArrayList<>();
//                String s = null;
//                if (alter_change.getBus_UpOrDown().equals("上行")){
//                    s = "0";
//                }else {s = "1";}
//                String line_n_upordown = alter_change.getLine_no() + "," + s;
//                String card_id = alter_change.getCard_no();
//                Integer now_stop_order = alter_change.getNow_stop_order();
//                if (alter_change.getLine_no() != null && now_stop_order != null && card_id != null) {
//                    MyRandom random = new MyRandom();
//                    int next_order = alter_change.getNow_stop_order() + 1;
//                    int erly_order = alter_change.getNow_stop_order() - 1;
//                    MyHashMap hp = alter_change.getHp();
//                    String nextget = jedis.hget(line_n_upordown, card_id + next_order);
//                    String eget = jedis.hget(line_n_upordown, card_id + erly_order);
//                    String u_off = jedis.hget(line_n_upordown, card_id + now_stop_order);
//                    if (u_off != null&&Integer.valueOf(u_off.split(",")[0])>now_stop_order) {
//                        alter_change.setOff_stop_order(Integer.valueOf(u_off.split(",")[0]));
//                        hp.put(Integer.valueOf(u_off.split(",")[0]), -1);
//                        alter_change.setHp(hp);
//                        alter_changes.add(alter_change);
//                    } else if (eget != null&&Integer.valueOf(eget.split(",")[0])>now_stop_order) {
//                        alter_change.setOff_stop_order(Integer.valueOf(eget.split(",")[0]));
//                        hp.put(Integer.valueOf(eget.split(",")[0]), -1);
//                        alter_change.setHp(hp);
//                        alter_changes.add(alter_change);
//                    } else if (nextget != null&&Integer.valueOf(nextget.split(",")[0])>now_stop_order) {
//                        alter_change.setOff_stop_order(Integer.valueOf(nextget.split(",")[0]));
//                        hp.put(Integer.valueOf(nextget.split(",")[0]), -1);
//                        alter_change.setHp(hp);
//                        alter_changes.add(alter_change);
//                    } else {
//                        int i1 = random.nextInt(7);
//                        Integer rd = null;
//                        rd = i1+alter_change.getNow_stop_order();
//                        if (rd>=alter_change.getEnd_station_order()){
//                            int i = alter_change.getEnd_station_order() - alter_change.getNow_stop_order();
//                            rd = random.nextInt(i + 1) + alter_change.getNow_stop_order();
//                        }
//                        hp.put(rd, -1);
//                        alter_change.setHp(hp);
//                        alter_changes.add(alter_change);
//                    }
//                }
//                else alter_changes.add(alter_change);
//                resultFuture.complete(alter_changes);
//            }
//
//            @Override
//            public void close() throws Exception {
//                super.close();
//                jedis.close();
//            }
//        }

new redisAsyncFC()

        , 10, TimeUnit.SECONDS, 1500).setParallelism(20)
                .filter(new FilterFunction<alter_change>() {
                    @Override
                    public boolean filter(alter_change alter_change) throws Exception {
                        if (alter_change.getNext_stop()!=null)return true;
                        else
                            return false;
                    }
                })

                /*每个刷卡上车都有一个相应的站点下车，将信息进行汇总*/
                .keyBy(new KeySelector<alter_change, String>() {
                    @Override
                    public String getKey(alter_change alter_change) throws Exception {
                        return alter_change.getLine_no() + alter_change.getBus_no() + alter_change.getBus_UpOrDown();
                    }
                })
                .reduce(new ReduceFunction<alter_change>() {
                    @Override
                    /**/
                    public alter_change reduce(alter_change LastValue, alter_change NowValue) throws Exception {
                        /*
                         * 处理乘客上车车辆实时载客量的累加
                         * 乘客关联到用户画像出来下车点后的逻辑
                         * 乘客未关联到用户画像的处理逻辑
                         * 车辆到达乘客下车点后hpmap以及实时载客量处理
                         * 车辆到达终点站后的实时载客量以及下车hashmap的初始化
                         * 车辆行驶途中改变上下行。清空hmap，及清空车上人数。
                         * */
                        MyHashMap LastHp = LastValue.getHp();
                        MyHashMap NowHp = NowValue.getHp();
                        if (NowValue.getNext_stop().equals("终点站")
//                                || NowValue.getNext_stop_order().equals(NowValue.getEnd_station_order())
                        ) {
//                        到达终点站初始化hp以及实时载客量count
                            LastValue.setCount(0);
                            LastHp.clear();
                            NowValue.setCount(0);
                            NowHp.clear();
                        } else {
//                      车辆行驶途中的业务
//                            上下行切换

//                            到达指定站点下车
                            if (NowValue.getNow_stop_order() != null) {
//                            由于请求超时存在找不到现在站点名称的车子
                                if (LastHp.containsKey(NowValue.getNow_stop_order())) {
                                    /*下车人数*/
                                    Integer integer = LastHp.get(NowValue.getNow_stop_order());
                                    LastHp.remove(NowValue.getNow_stop_order());
                                    Integer count = LastValue.getCount();
                                    count = count + integer;
                                    LastValue.setCount(count);
                                }

                            }
                            NowValue.setCount(LastValue.getCount() + NowValue.getCount());
                            NowHp.putAtoB(LastHp);
                        }
                        return NowValue;
                    }
                }).setParallelism(3);
        alter_change_output
                .map(new MapFunction<alter_change, String>() {
                    @Override
                    public String map(alter_change alter_change) throws Exception {
                        Gson gson = new Gson();
                        String s = gson.toJson(alter_change);
                        return s;
                    }
                })
//                .print();


                .addSink(new KafKaSink().SinkToKafKa()).setParallelism(3).name("上车关联用户画像结果输出到kafka");

        alter_change_output.filter(new FilterFunction<alter_change>() {
            @Override
            public boolean filter(alter_change alter_change) throws Exception {
                if (alter_change.getCount()>8)return true;
                return false;
            }
        }).addSink(new MySqlSinkFc()).name("sink到mysql");

        env.execute();
    }
}
