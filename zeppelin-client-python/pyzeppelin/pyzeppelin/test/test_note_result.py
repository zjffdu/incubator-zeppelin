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
from pyzeppelin.result import NoteResult
import json

class TestNoteResult(unittest.TestCase):

    def test_note(self):
        note_json = """
        {"paragraphs":[{"title":"run sh","text":"%sh echo \u0027hello world\u0027",
        "user":"anonymous","dateUpdated":"Nov 23, 2020 3:15:14 PM","progress":0,"config":{},
        "settings":{"params":{},"forms":{}},"apps":[],"runtimeInfos":{},"progressUpdateIntervalMs":500,
        "jobName":"paragraph_1606115714058_1880824475","id":"paragraph_1606115714058_1880824475",
        "dateCreated":"Nov 23, 2020 3:15:14 PM","status":"PENDING"}],"name":"note_3","id":"2FRY2GX26",
        "defaultInterpreterGroup":"spark","version":"0.9.0-SNAPSHOT","noteParams":{},"noteForms":{},
        "angularObjects":{},"config":{"isZeppelinNotebookCronEnable":false},
        "info":{"inIsolatedMode":true,"isRunning":true,"startTime":"2020-11-23_15-15-14"},"path":"/test/note_3"}
        """
        note = NoteResult(json.loads(note_json))
        self.assertEqual('2FRY2GX26', note.note_id)
        self.assertEqual(True, note.is_running)
        self.assertEqual(1, len(note.paragraphs))
        self.assertEqual('PENDING', note.paragraphs[0].status)
        self.assertEqual(0, note.paragraphs[0].progress)
        self.assertEqual('paragraph_1606115714058_1880824475', note.paragraphs[0].paragraph_id)
        self.assertEqual(0, len(note.paragraphs[0].results))


if __name__ == '__main__':
    unittest.main()
