package org.apache.zeppelin.flink

import org.apache.flink.client.program.ClusterClient
import org.apache.flink.runtime.minicluster.MiniCluster

object types {
  type ClusterType = Option[Either[MiniCluster, ClusterClient[_]]]
}
