package org.apache.spark.util

import java.io.OutputStream

import scala.concurrent.duration.{FiniteDuration, _}

class BenchmarkWrapper(
  name: String,
  valuesPerIteration: Long,
  minNumIters: Int = 1,
  warmupTime: FiniteDuration = 0.seconds,
  minTime: FiniteDuration = 0.seconds,
  outputPerIteration: Boolean = false,
  output: Option[OutputStream] = None)
  extends Benchmark(
    name = name,
    valuesPerIteration = valuesPerIteration,
    minNumIters = minNumIters,
    warmupTime = warmupTime,
    minTime = minTime,
    outputPerIteration = outputPerIteration,
    output = output) {

}
