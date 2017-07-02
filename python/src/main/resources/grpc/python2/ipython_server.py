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

import sys
import time
from concurrent import futures

import grpc
import ipython_pb2
import ipython_pb2_grpc

_ONE_DAY_IN_SECONDS = 60 * 60 * 24

import jupyter_client
from IPython.utils.capture import capture_output

TIMEOUT = 30

class IPython(ipython_pb2_grpc.IPythonServicer):

    def __init__(self, server):
        self.km, self.kc = jupyter_client.manager.start_new_kernel(kernel_name='python')
        self.server = server

    def execute(self, request, context):
        with capture_output() as io:
            reply = self.kc.execute_interactive(request.code, timeout=TIMEOUT)

        if reply['content']['status'] == 'ok':
            if io.stdout:
                return ipython_pb2.ExecuteResponse(status=ipython_pb2.SUCCESS, result=io.stdout)
            else:
                result = []
                for payload in reply['content']['payload']:
                    if payload['data']['text/plain']:
                        result.append(payload['data']['text/plain'])
                return ipython_pb2.ExecuteResponse(status=ipython_pb2.SUCCESS,
                                                   result='\n'.join(result))
        else:
            return ipython_pb2.ExecuteResponse(status=ipython_pb2.ERROR, result=io.stderr)


    def complete(self, request, context):
        reply = self.kc.complete(request.code, reply=True, timeout=TIMEOUT)
        return ipython_pb2.CompleteResponse(matches=reply['content']['matches'])

    def stop(self, request, context):
        self.server.stop(0)
        sys.exit(0)


def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ipython_pb2_grpc.add_IPythonServicer_to_server(IPython(server), server)
    server.add_insecure_port('[::]:' + port)
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve(sys.argv[1])
