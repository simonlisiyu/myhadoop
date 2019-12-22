package com.lsy.myhadoop.spark2.kryo;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Created by lisiyu on 2018/5/18.
 */
public class OrderTrajPbFactory {
    private static Log LOG = LogFactory.getLog(OrderTrajPbFactory.class);

//    private static final OrderTrajPb.OrderTraj.Builder OrderTrajPbBuilder = OrderTrajPb.OrderTraj.newBuilder();
//    private static final OrderTrajPb.map_match_point_pb.Builder pointPbBuilder = OrderTrajPb.map_match_point_pb.newBuilder();

    public static OrderTrajPb.OrderTraj buildOrderTrajPb(Iterable<OrderTrajPb.map_match_point_pb> list){
        OrderTrajPb.OrderTraj.Builder OrderTrajPbBuilder = OrderTrajPb.OrderTraj.newBuilder();
        for(OrderTrajPb.map_match_point_pb point_pb : list){
            OrderTrajPbBuilder.addPointList(point_pb);
        }
        OrderTrajPb.OrderTraj traj = OrderTrajPbBuilder.build();
        return traj;
    }
    public static OrderTrajPb.OrderTraj buildOrderTrajPb(String userId, Iterable<OrderTrajPb.map_match_point_pb> list){
        OrderTrajPb.OrderTraj.Builder OrderTrajPbBuilder = OrderTrajPb.OrderTraj.newBuilder();
        for(OrderTrajPb.map_match_point_pb point_pb : list){
            OrderTrajPbBuilder.addPointList(point_pb);
        }
        OrderTrajPbBuilder.setUserId(userId);
        OrderTrajPb.OrderTraj traj = OrderTrajPbBuilder.build();
        return traj;
    }
    public static OrderTrajPb.OrderTraj buildOrderTrajPb(String userId, Iterable<OrderTrajPb.map_match_point_pb> list,
                                                         int startX, int startY, int endX, int endY,
                                                         int startTs, int endTs){
        OrderTrajPb.OrderTraj.Builder OrderTrajPbBuilder = OrderTrajPb.OrderTraj.newBuilder();
        for(OrderTrajPb.map_match_point_pb point_pb : list){
            OrderTrajPbBuilder.addPointList(point_pb);
        }
        OrderTrajPbBuilder.setUserId(userId);
        OrderTrajPbBuilder.setStartX(startX);
        OrderTrajPbBuilder.setStartY(startY);
        OrderTrajPbBuilder.setStartTs(startTs);
        OrderTrajPbBuilder.setEndX(endX);
        OrderTrajPbBuilder.setEndY(endY);
        OrderTrajPbBuilder.setEndTs(endTs);
        OrderTrajPb.OrderTraj traj = OrderTrajPbBuilder.build();
        return traj;
    }

    public static byte[] buildBytesOfOrderTrajPb(Iterable<OrderTrajPb.map_match_point_pb> list){
        OrderTrajPb.OrderTraj traj = buildOrderTrajPb(list);
        byte[] bytes = traj.toByteArray();
        return bytes;
    }

    public static OrderTrajPb.OrderTraj parseBytesToOrderTrajPb(byte[] bytes) throws InvalidProtocolBufferException {
        OrderTrajPb.OrderTraj traj = OrderTrajPb.OrderTraj.parseFrom(bytes);
        return traj;
    }

    public static OrderTrajPb.map_match_point_pb buildMMPointPb(int projX, int projY,
                                                              int timestamp, Iterable<Long> linkIdVec,
                                                              int linkPassDist, int srcX, int srcY,
                                                              int bizType, int mapVersion, int bizStatus){
        OrderTrajPb.map_match_point_pb.Builder pointPbBuilder = OrderTrajPb.map_match_point_pb.newBuilder();

        pointPbBuilder.setProjX(projX);
        pointPbBuilder.setProjY(projY);
        pointPbBuilder.setTimestamp(timestamp);
        for(long linkId : linkIdVec){
            pointPbBuilder.addLinkIdVec(linkId);
        }
        pointPbBuilder.setLinkPassDist(linkPassDist);

        pointPbBuilder.setSrcX(srcX);
        pointPbBuilder.setSrcY(srcY);
        pointPbBuilder.setBiztype(bizType);
        pointPbBuilder.setMapVersion(mapVersion);
        pointPbBuilder.setBizstatus(bizStatus);
//        pointPbBuilder.setPointSpeed(0.0f);
//        pointPbBuilder.setPointDirection(0.0f);
//        pointPbBuilder.setLineSpeed(0.0f);
//        pointPbBuilder.setLineDirection(0.0f);
//        pointPbBuilder.setCertainty(0);
//        pointPbBuilder.setPhone(1);

        OrderTrajPb.map_match_point_pb mmp = pointPbBuilder.build();
        return mmp;
    }

    public static byte[] buildBytesOfMMPointPb(int projX, int projY,
                                               int timestamp, Iterable<Long> linkIdVec,
                                               int linkPassDist, int srcX, int srcY,
                                               int bizType, int mapVersion, int bizStatus){
        OrderTrajPb.map_match_point_pb mmp = buildMMPointPb(projX, projY, timestamp,
                linkIdVec, linkPassDist, srcX, srcY, bizType, mapVersion, bizStatus);
        byte[] bytes = mmp.toByteArray();
        return bytes;
    }

    public static OrderTrajPb.map_match_point_pb parseBytesToMMPointPb(byte[] bytes) throws InvalidProtocolBufferException {
        OrderTrajPb.map_match_point_pb point_pb = OrderTrajPb.map_match_point_pb.parseFrom(bytes);
        return point_pb;
    }

}
