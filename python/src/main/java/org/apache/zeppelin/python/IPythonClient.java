/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.zeppelin.python;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Grpc client for IPython kernel
 */
public class IPythonClient {

  private static final Logger logger = LoggerFactory.getLogger(IPythonClient.class.getName());

  private final ManagedChannel channel;
  private final IPythonGrpc.IPythonBlockingStub blockingStub;
  private final IPythonGrpc.IPythonStub asyncStub;

  private Random random = new Random();

  /**
   * Construct client for accessing RouteGuide server at {@code host:port}.
   */
  public IPythonClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
  }

  /**
   * Construct client for accessing RouteGuide server using the existing channel.
   */
  public IPythonClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    blockingStub = IPythonGrpc.newBlockingStub(channel);
    asyncStub = IPythonGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  public ExecuteResponse execute(ExecuteRequest request) {
    return blockingStub.execute(request);
  }

  public CompleteResponse complete(CompleteRequest request) {
    return blockingStub.complete(request);
  }

  public void stop(StopRequest request) {
    asyncStub.stop(request, null);
  }

  public static void main(String[] args) {
    IPythonClient client = new IPythonClient("localhost", 50053);
    ExecuteResponse response = client.execute(ExecuteRequest.newBuilder().
        setCode("abcd=2").build());
    System.out.println(response.getResult());

    CompleteResponse completeResponse = client.complete(
        CompleteRequest.newBuilder().setCode("ab").build());
    for (int i = 0; i < completeResponse.getMatchesCount(); ++i) {
      System.out.println(completeResponse.getMatches(i));
    }

  }
}
