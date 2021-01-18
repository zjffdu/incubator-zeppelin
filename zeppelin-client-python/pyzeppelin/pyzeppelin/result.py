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



class NoteResult:

    def __init__(self, note_json):
        self.note_id = note_json['id']
        self.is_running = False
        if 'info' in note_json:
            info_json = note_json['info']
            if 'isRunning' in info_json:
                self.is_running = bool(info_json['isRunning'])
        self.paragraphs = []
        if 'paragraphs' in note_json:
            paragraph_json_array = note_json['paragraphs']
            self.paragraphs = list(map(lambda x : ParagraphResult(x), paragraph_json_array))

    def is_success(self):
        for p in self.paragraphs:
            if p.status != 'FINISHED':
                return False;
        return True

    def __repr__(self):
        return str(self.__dict__)



class ParagraphResult:

    def __init__(self, paragraph_json):
        self.paragraph_id = paragraph_json['id']
        self.status = paragraph_json['status']
        self.progress = 0
        if 'progress' in paragraph_json:
            self.progress = int(paragraph_json['progress'])
        if 'results' in paragraph_json:
            results_json = paragraph_json['results']
            msg_array = results_json['msg']
            self.results = list(map(lambda x : (x['type'], x['data']), msg_array))
        else:
            self.results = []

        self.jobUrls = []
        if 'runtimeInfos' in paragraph_json:
            runtimeInfos_json = paragraph_json['runtimeInfos']
            if 'jobUrl' in runtimeInfos_json:
                jobUrl_json = runtimeInfos_json['jobUrl']
                if 'values' in jobUrl_json:
                    jobUrl_values = jobUrl_json['values']
                    self.jobUrls = list(map(lambda x: x['jobUrl'], filter(lambda x : 'jobUrl' in x, jobUrl_values)))


    def is_completed(self):
        return self.status in ['FINISHED', 'ERROR', 'ABORTED']

    def is_running(self):
        return self.status == 'RUNNING'

    def __repr__(self):
        return str(self.__dict__)


class ExecuteResult:

    def __init__(self, paragraph_result):
        self.statement_id = paragraph_result.paragraph_id
        self.status = paragraph_result.status
        self.progress = paragraph_result.progress
        self.results = paragraph_result.results
        self.jobUrls = paragraph_result.jobUrls

    def __repr__(self):
        return str(self.__dict__)

    def is_success(self):
        return self.status == 'FINISHED'

