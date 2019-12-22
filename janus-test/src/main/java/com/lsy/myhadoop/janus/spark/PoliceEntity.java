package com.lsy.myhadoop.janus.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
get police data and write it to janusgraph
 */
@Data
@AllArgsConstructor
@NoArgsConstructor

public class PoliceEntity {
    private String policeId;
    private String name;
}
