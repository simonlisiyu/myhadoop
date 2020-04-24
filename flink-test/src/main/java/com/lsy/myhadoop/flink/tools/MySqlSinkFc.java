package com.lsy.myhadoop.flink.tools;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.lsy.myhadoop.flink.domain.alter_change;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class MySqlSinkFc extends RichSinkFunction<alter_change> {
    DruidPooledConnection getconnectionpool = null;
    PreparedStatement preparedStatement = null;
    PreparedStatement deletestatement = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        getconnectionpool = new GetDBConnect().getconnectionpool();
        preparedStatement = getconnectionpool.prepareStatement("INSERT INTO bus_status (line_code, bus_id, upordown, " +
                "now_station_order, now_station_name, next_station_order, next_station_name, ccount, update_time) " +
                "VALUE (?, ?, ?, ?, ?, ? ,? ,? ,?)");
        deletestatement = getconnectionpool.prepareStatement("delete from bus_status where update_time < ?");
    }

    @Override
    public void invoke(alter_change value, Context context) throws Exception {
        if (getconnectionpool.isClosed())
        {
            getconnectionpool = new GetDBConnect().getconnectionpool();
            preparedStatement = getconnectionpool.prepareStatement("INSERT INTO bus_status (line_code, bus_id, upordown, " +
                    "now_station_order, now_station_name, next_station_order, next_station_name, ccount, update_time) " +
                    "VALUE (?, ?, ?, ?, ?, ? ,? ,? ,?)");
            deletestatement = getconnectionpool.prepareStatement("delete from bus_status where update_time < ?");
        }
        Timestamp timest = Timestamp.valueOf(value.getM_time());
        LocalDateTime dateTime = timest.toLocalDateTime();
        LocalDateTime old = dateTime.minusDays(2);
        String format = old.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"));
        String format1 = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"));
        Timestamp old_date = Timestamp.valueOf(format);
        Timestamp timestamp = Timestamp.valueOf(format1);
        preparedStatement.setString(1, value.getLine_no());
        preparedStatement.setString(2, value.getBus_no());
        preparedStatement.setString(3,value.getBus_UpOrDown());
        if (value.getNext_stop_order()!=null){
            preparedStatement.setInt(4, value.getNext_stop_order()-1);
            preparedStatement.setInt(6, value.getNext_stop_order());
        }else {
            preparedStatement.setInt(4, 0);
            preparedStatement.setInt(6, 0);
        }
        preparedStatement.setString(5, value.getNow_stop_name());
        preparedStatement.setString(7, value.getNext_stop());
        preparedStatement.setInt(8, value.getCount());
        preparedStatement.setObject(9, value.getM_time());
        deletestatement.setTimestamp(1, old_date);
        deletestatement.executeUpdate();
        preparedStatement.executeUpdate();

    }

    @Override
    public void close() throws Exception {
        if (getconnectionpool!=null){
            getconnectionpool.close();
        }
        if (preparedStatement!=null||deletestatement!=null){
            preparedStatement.close();
            deletestatement.close();
        }
    }
}
