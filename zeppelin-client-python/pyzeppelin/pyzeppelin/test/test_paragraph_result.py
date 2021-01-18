#
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

import unittest
from pyzeppelin.result import ParagraphResult
import json

class TestParagraphResult(unittest.TestCase):

    def test_paragraph(self):
        paragraph_json = """
        {"title":"run sh","text":"%sh echo hello world","user":"anonymous",
        "dateUpdated":"Nov 23, 2020 2:38:05 PM","progress":0,"config":{},
        "settings":{"params":{},"forms":{}},
        "results":{"code":"SUCCESS","msg":[{"type":"TEXT","data":"hello world"}]},
        "apps":[],"runtimeInfos":{},"progressUpdateIntervalMs":500,
        "jobName":"paragraph_1606113485255_664873269",
        "id":"paragraph_1606113485255_664873269",
        "dateCreated":"Nov 23, 2020 2:38:05 PM","dateStarted":"Nov 23, 2020 2:38:05 PM",
        "dateFinished":"Nov 23, 2020 2:44:52 PM","status":"FINISHED"}
        """
        p = ParagraphResult(json.loads(paragraph_json))
        self.assertEqual('paragraph_1606113485255_664873269', p.paragraph_id)
        self.assertEqual('FINISHED', p.status)
        self.assertEqual(0, p.progress)
        self.assertEqual(1, len(p.results))
        self.assertEqual('TEXT', p.results[0][0])
        self.assertEqual('hello world', p.results[0][1])
        self.assertEqual(0, len(p.jobUrls))


if __name__ == '__main__':
    unittest.main()
