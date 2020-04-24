package com.lsy.myhadoop.flink.domain;

public class bus_pojo {
    String mtime;
    String stime;
    String line_no;
    String bus_no;
    String car_type;
    String now_stop;
    Integer now_stop_order;
    String next_stop;
    Integer next_stop_order;
    String up_or_down;
    String bus_status;
    Integer end_station_order;
    //    车辆座位数
    String seat_rang;
    //    车辆额定载客数
    String passanger_capacity;

    public String getMtime() {
        return mtime;
    }

    public void setMtime(String mtime) {
        this.mtime = mtime;
    }

    public String getStime() {
        return stime;
    }

    public void setStime(String stime) {
        this.stime = stime;
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

    public String getNow_stop() {
        return now_stop;
    }

    public void setNow_stop(String now_stop) {
        this.now_stop = now_stop;
    }

    public Integer getNow_stop_order() {
        return now_stop_order;
    }

    public void setNow_stop_order(Integer now_stop_order) {
        this.now_stop_order = now_stop_order;
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

    public String getUp_or_down() {
        return up_or_down;
    }

    public void setUp_or_down(String up_or_down) {
        this.up_or_down = up_or_down;
    }

    public String getBus_status() {
        return bus_status;
    }

    public void setBus_status(String bus_status) {
        this.bus_status = bus_status;
    }

    public Integer getEnd_station_order() {
        return end_station_order;
    }

    public void setEnd_station_order(Integer end_station_order) {
        this.end_station_order = end_station_order;
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

    @Override
    public String toString() {
        return "bus_pojo{" +
                "mtime='" + mtime + '\'' +
                ", stime='" + stime + '\'' +
                ", line_no='" + line_no + '\'' +
                ", bus_no='" + bus_no + '\'' +
                ", car_type='" + car_type + '\'' +
                ", now_stop='" + now_stop + '\'' +
                ", now_stop_order=" + now_stop_order +
                ", next_stop='" + next_stop + '\'' +
                ", next_stop_order=" + next_stop_order +
                ", up_or_down='" + up_or_down + '\'' +
                ", bus_status='" + bus_status + '\'' +
                ", end_station_order=" + end_station_order +
                ", seat_rang='" + seat_rang + '\'' +
                ", passanger_capacity='" + passanger_capacity + '\'' +
                '}';
    }
}
