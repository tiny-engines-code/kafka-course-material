package com.chrislomeli.kafkautil.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collection;
import java.util.Set;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClusterInfo {
    String id;
    String hostInfo;
    Collection<AclOperation> acl;
}
