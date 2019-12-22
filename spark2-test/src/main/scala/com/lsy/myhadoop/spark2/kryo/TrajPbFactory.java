package com.lsy.myhadoop.spark2.kryo;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Created by lisiyu on 2018/5/18.
 */
public class TrajPbFactory {
    private static Log LOG = LogFactory.getLog(TrajPbFactory.class);

//    private static final TrajPb.OrderTraj.Builder trajPbBuilder = TrajPb.OrderTraj.newBuilder();
//    private static final TrajPb.map_match_point_pb.Builder pointPbBuilder = TrajPb.map_match_point_pb.newBuilder();

    public static TrajPb.OrderTraj buildOrderTrajPb(Iterable<TrajPb.map_match_point_pb> list){
        TrajPb.OrderTraj.Builder trajPbBuilder = TrajPb.OrderTraj.newBuilder();
        for(TrajPb.map_match_point_pb point_pb : list){
            trajPbBuilder.addPointList(point_pb);
        }
        TrajPb.OrderTraj traj = trajPbBuilder.build();
        return traj;
    }

    public static byte[] buildBytesOfOrderTrajPb(Iterable<TrajPb.map_match_point_pb> list){
        TrajPb.OrderTraj traj = buildOrderTrajPb(list);
        byte[] bytes = traj.toByteArray();
        return bytes;
    }

    public static TrajPb.OrderTraj parseBytesToOrderTrajPb(byte[] bytes) throws InvalidProtocolBufferException {
        TrajPb.OrderTraj traj = TrajPb.OrderTraj.parseFrom(bytes);
        return traj;
    }

    public static TrajPb.map_match_point_pb buildMMPointPb(String userId, int projX, int projY,
                                                              int timestamp, Iterable<Long> linkIdVec,
                                                              int linkPassDist, int srcX, int srcY,
                                                              int bizType, int mapVersion, int bizStatus){
        TrajPb.map_match_point_pb.Builder pointPbBuilder = TrajPb.map_match_point_pb.newBuilder();

        pointPbBuilder.setUserId(userId);
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

        TrajPb.map_match_point_pb mmp = pointPbBuilder.build();
        return mmp;
    }

    public static byte[] buildBytesOfMMPointPb(String userId, int projX, int projY,
                                               int timestamp, Iterable<Long> linkIdVec,
                                               int linkPassDist, int srcX, int srcY,
                                               int bizType, int mapVersion, int bizStatus){
        TrajPb.map_match_point_pb mmp = buildMMPointPb(userId, projX, projY, timestamp,
                linkIdVec, linkPassDist, srcX, srcY, bizType, mapVersion, bizStatus);
        byte[] bytes = mmp.toByteArray();
        return bytes;
    }

    public static TrajPb.map_match_point_pb parseBytesToMMPointPb(byte[] bytes) throws InvalidProtocolBufferException {
        TrajPb.map_match_point_pb point_pb = TrajPb.map_match_point_pb.parseFrom(bytes);
        return point_pb;
    }

}
