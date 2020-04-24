package com.lsy.myhadoop.flink.domain;


import com.lsy.myhadoop.flink.tools.MyHashMap;

public class alter_change {
    public alter_change() {
    }

    public alter_change(bus_pojo busPojo) {
        this.m_time = busPojo.getMtime();
        this.s_time = busPojo.getStime();
        this.line_no = busPojo.getLine_no();
        this.bus_no = busPojo.getBus_no();
        this.bus_UpOrDown = busPojo.getUp_or_down();
        this.Now_stop_name = busPojo.getNow_stop();
        this.bus_status = busPojo.getBus_status();
        this.Now_stop_order = busPojo.getNow_stop_order();
        this.next_stop = busPojo.getNext_stop();
        this.next_stop_order = busPojo.getNext_stop_order();
        this.end_station_order = busPojo.getEnd_station_order();
        this.car_type = busPojo.getCar_type();
        this.seat_rang = busPojo.getSeat_rang();
        this.passanger_capacity = busPojo.getPassanger_capacity();
    }

    //    车辆时间
    String m_time;
//    发车时间
    String s_time;
//    线路
    String line_no;
//    车辆编号
    String bus_no;
    //    车辆型号
    String car_type;
    //    车辆座位数
    String seat_rang;
    //    车辆额定载客数
    String passanger_capacity;
//    上下行
    String bus_UpOrDown;
//    刷卡人
    String card_no;
//    现在站点名称
    String Now_stop_name;
//    巴士状态{运营/非运营}
    String bus_status;
//    现在站点序号
    Integer Now_stop_order;
//    下一站点名称
    String next_stop;
//    下一站点序号
    Integer next_stop_order;
//    终点站名称
    Integer end_station_order;
//    下车站点
    Integer off_stop_order;
//    记录下车点，下车人数的hashmap
    MyHashMap hp;
//    人数
    Integer count;

    public String getM_time() {
        return m_time;
    }

    public void setM_time(String m_time) {
        this.m_time = m_time;
    }

    public String getS_time() {
        return s_time;
    }

    public void setS_time(String s_time) {
        this.s_time = s_time;
    }

    public String getLine_no() {
        return line_no;
    }

    public void setLine_no(String line_no) {
        this.line_no = line_no;
    }

    public String getBus_no() {
        return bus_no;
    }

    public void setBus_no(String bus_no) {
        this.bus_no = bus_no;
    }

    public String getCar_type() {
        return car_type;
    }

    public void setCar_type(String car_type) {
        this.car_type = car_type;
    }

    public String getSeat_rang() {
        return seat_rang;
    }

    public void setSeat_rang(String seat_rang) {
        this.seat_rang = seat_rang;
    }

    public String getPassanger_capacity() {
        return passanger_capacity;
    }

    public void setPassanger_capacity(String passanger_capacity) {
        this.passanger_capacity = passanger_capacity;
    }

    public String getBus_UpOrDown() {
        return bus_UpOrDown;
    }

    public void setBus_UpOrDown(String bus_UpOrDown) {
        this.bus_UpOrDown = bus_UpOrDown;
    }

    public String getCard_no() {
        return card_no;
    }

    public void setCard_no(String card_no) {
        this.card_no = card_no;
    }

    public String getNow_stop_name() {
        return Now_stop_name;
    }

    public void setNow_stop_name(String now_stop_name) {
        Now_stop_name = now_stop_name;
    }

    public String getBus_status() {
        return bus_status;
    }

    public void setBus_status(String bus_status) {
        this.bus_status = bus_status;
    }

    public Integer getNow_stop_order() {
        return Now_stop_order;
    }

    public void setNow_stop_order(Integer now_stop_order) {
        Now_stop_order = now_stop_order;
    }

    public String getNext_stop() {
        return next_stop;
    }

    public void setNext_stop(String next_stop) {
        this.next_stop = next_stop;
    }

    public Integer getNext_stop_order() {
        return next_stop_order;
    }

    public void setNext_stop_order(Integer next_stop_order) {
        this.next_stop_order = next_stop_order;
    }

    public Integer getEnd_station_order() {
        return end_station_order;
    }

    public void setEnd_station_order(Integer end_station_order) {
        this.end_station_order = end_station_order;
    }

    public Integer getOff_stop_order() {
        return off_stop_order;
    }

    public void setOff_stop_order(Integer off_stop_order) {
        this.off_stop_order = off_stop_order;
    }

    public MyHashMap getHp() {
        return hp;
    }

    public void setHp(MyHashMap hp) {
        this.hp = hp;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "alter_change{" +
                "m_time='" + m_time + '\'' +
                ", s_time='" + s_time + '\'' +
                ", line_no='" + line_no + '\'' +
                ", bus_no='" + bus_no + '\'' +
                ", car_type='" + car_type + '\'' +
                ", seat_rang='" + seat_rang + '\'' +
                ", passanger_capacity='" + passanger_capacity + '\'' +
                ", bus_UpOrDown='" + bus_UpOrDown + '\'' +
                ", card_no='" + card_no + '\'' +
                ", Now_stop_name='" + Now_stop_name + '\'' +
                ", bus_status='" + bus_status + '\'' +
                ", Now_stop_order=" + Now_stop_order +
                ", next_stop='" + next_stop + '\'' +
                ", next_stop_order=" + next_stop_order +
                ", end_station_order=" + end_station_order +
                ", off_stop_order=" + off_stop_order +
                ", hp=" + hp +
                ", count=" + count +
                '}';
    }
}
