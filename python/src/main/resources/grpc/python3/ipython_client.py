# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import grpc

import ipython_pb2
import ipython_pb2_grpc


def run():
    channel = grpc.insecure_channel('localhost:50053')
    stub = ipython_pb2_grpc.IPythonStub(channel)
    response = stub.execute(ipython_pb2.ExecuteRequest(code="print('hello world')"))
    print("Greeter client received: " + response.result)
    response = stub.execute(ipython_pb2.ExecuteRequest(code="abcd=12"))
    print("Greeter client received: " + response.result)
    response = stub.complete(ipython_pb2.CompleteRequest(code="ab"))
    print("Complete client received: " + str(response.matches))

if __name__ == '__main__':
    run()
