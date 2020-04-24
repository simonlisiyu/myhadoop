package com.lsy.myhadoop.flink.tools;

import com.lsy.myhadoop.flink.domain.bus_pojo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class mysqlAsyncFC extends RichAsyncFunction<bus_pojo, bus_pojo> {
    private transient MysqlClient mysqlClient;
    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mysqlClient = new MysqlClient();
        executorService = Executors.newFixedThreadPool(80);
    }

    @Override
    public void asyncInvoke(bus_pojo busPojo, ResultFuture<bus_pojo> resultFuture) throws Exception {
//        判断是否为终点站，若否则不进行查询。
        if (!busPojo.getNext_stop().equals("--")||!busPojo.getNext_stop().equals("终点站")){
        executorService.submit(() -> {
            // submit query
            bus_pojo bus_pojo = mysqlClient.query1(busPojo);
            // 一定要记得放回 resultFuture，不然数据全部是timeout 的
            resultFuture.complete(Collections.singletonList(bus_pojo));
        });}
        else {
            busPojo.setStime("终点站车辆，不计算发车时间");
            resultFuture.complete(Collections.singleton(busPojo));
        }


    }

    @Override
    public void timeout(bus_pojo input, ResultFuture<bus_pojo> resultFuture) throws Exception {
        ArrayList<bus_pojo> bus_pojos = new ArrayList<>();
        input.setStime("查询超时，未能获取班次信息");
        bus_pojos.add(input);
//        System.out.println(input.toString());
        resultFuture.complete(bus_pojos);
//        System.out.println(input.getBus_no()+input.getNext_stop()+"------请求超时");
    }


    @Override
    public void close() throws Exception {
        super.close();
    }

}
