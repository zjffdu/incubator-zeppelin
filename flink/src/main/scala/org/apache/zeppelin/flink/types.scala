package org.apache.zeppelin.flink

import org.apache.flink.client.program.ClusterClient
import org.apache.flink.runtime.minicluster.{MiniCluster, StandaloneMiniCluster}

object types {
  type LocalCluster = Either[StandaloneMiniCluster, MiniCluster]
  type ClusterType = Option[Either[LocalCluster, ClusterClient[_]]]
}
