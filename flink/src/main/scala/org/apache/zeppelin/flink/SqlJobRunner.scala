package org.apache.zeppelin.flink

import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.table.api.TableEnvironment
import org.slf4j.{Logger, LoggerFactory}

class SqlJobRunner(cluster: types.ClusterType,
                   jobGraph: JobGraph,
                   jobName: String,
                   classLoader: ClassLoader) {

  lazy val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  def run(): Unit = {
    cluster match {
      case Some(Left(Left(legacyMiniCluster))) =>
        throw new RuntimeException("LegacyMiniCluster is not supported")
      case Some(Left(Right(newMiniCluster))) =>
        newMiniCluster.executeJob(jobGraph, false)
      case Some(Right(clusterClient)) =>
        val jobResult = clusterClient.submitJob(jobGraph, classLoader).getJobExecutionResult
        jobResult.getAllAccumulatorResults()
      case e =>
        LOGGER.error("Unrecognized cluster type: " + e.getClass.getSimpleName)
    }

  }

}
