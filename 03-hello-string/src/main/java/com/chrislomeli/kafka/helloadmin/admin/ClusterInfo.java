package com.chrislomeli.kafka.helloadmin.admin;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.acl.AclOperation;

import java.util.Collection;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClusterInfo {
    String id;
    String hostInfo;
    Collection<AclOperation> acl;
}
