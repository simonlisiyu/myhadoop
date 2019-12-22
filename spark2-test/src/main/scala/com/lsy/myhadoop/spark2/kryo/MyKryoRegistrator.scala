package com.lsy.myhadoop.spark2.kryo

import java.util.Collections

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsEmptyListSerializer
import de.javakaffee.kryoserializers.protobuf.ProtobufSerializer
import org.apache.spark.serializer.KryoRegistrator

/**
  * Created by lisiyu on 2018/5/21.
  */
class MyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register( Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer() );
    // Probably should use proto serializer for your proto classes
    kryo.register(classOf[TrajPb.OrderTraj], new ProtobufSerializer() );
    kryo.register(classOf[TrajPb.map_match_point_pb], new ProtobufSerializer() );

    kryo.register(classOf[OrderTrajPb.OrderTraj], new ProtobufSerializer() );
    kryo.register(classOf[OrderTrajPb.map_match_point_pb], new ProtobufSerializer() );
  }
}
