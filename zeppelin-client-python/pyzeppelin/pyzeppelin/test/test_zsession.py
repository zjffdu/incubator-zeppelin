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

from pyzeppelin.config import ClientConfig
from pyzeppelin.zsession import ZSession


class TestZeppelinClient(unittest.TestCase):

    def test_zsession_sh_execute(self):
        client_config = ClientConfig("http://localhost:8080")
        session = ZSession(client_config, 'sh')
        # session.login('user1', 'password2')

        try:
            session.start()
            self.assertIsNotNone(session.session_info.session_id)

            result = session.execute("echo 'hello world'")
            self.assertEqual('FINISHED', result.status)
            self.assertEqual(1, len(result.results))
            self.assertEqual('TEXT', result.results[0][0])
            self.assertEqual('hello world\n', result.results[0][1])

            result = session.execute("invalid_command")
            self.assertEqual('ERROR', result.status)
        finally:
            session.stop()


    def test_zsession_sh_submit(self):
        client_config = ClientConfig("http://localhost:8080")
        session = ZSession(client_config, 'sh')
        # session.login('user1', 'password2')

        try:
            session.start()
            self.assertIsNotNone(session.session_info.session_id)

            result = session.submit("echo 'hello world'")
            result = session.wait_util_finished(result.statement_id)
            self.assertEqual('FINISHED', result.status)
            self.assertEqual(1, len(result.results))
            self.assertEqual('TEXT', result.results[0][0])
            self.assertEqual('hello world\n', result.results[0][1])

            result = session.submit("invalid_command")
            result = session.wait_util_finished(result.statement_id)
            self.assertEqual('ERROR', result.status)
        finally:
            session.stop()

    def test_zsession_spark_execute(self):
        client_config = ClientConfig("http://localhost:8080")
        session = ZSession(client_config, 'spark', intp_properties= {'spark.master': 'local[*]'})
        #session.login('user1', 'password2')

        try:
            session.start()
            self.assertIsNotNone(session.session_info.session_id)
            self.assertIsNotNone(session.session_info.weburl)

            result = session.execute("sc.version")
            self.assertEqual('FINISHED', result.status)
            self.assertEqual(1, len(result.results))
            self.assertEqual(0, len(result.jobUrls))

            result = session.execute("df = spark.createDataFrame([(1,'a'),(2,'b')])\n" +
                                        "df.registerTempTable('df')\n" +
                                        "df.show()", sub_interpreter= "pyspark")
            self.assertEqual('FINISHED', result.status)
            self.assertEqual(1, len(result.results))
            self.assertTrue(len(result.jobUrls) > 0)
            self.assertEqual('TEXT', result.results[0][0])
        finally:
            session.stop()

    def test_zsession_spark_submit(self):
        client_config = ClientConfig("http://localhost:8080")
        session = ZSession(client_config, 'spark', intp_properties= {'spark.master': 'local[*]'})
        # session.login('user1', 'password2')

        try:
            session.start()
            self.assertIsNotNone(session.session_info.session_id)
            self.assertIsNotNone(session.session_info.weburl)

            result = session.submit("sc.version")
            result = session.wait_util_finished(result.statement_id)
            self.assertEqual('FINISHED', result.status)
            self.assertEqual(1, len(result.results))
            self.assertEqual(0, len(result.jobUrls))

            result = session.submit("df = spark.createDataFrame([(1,'a'),(2,'b')])\n" +
                                        "df.registerTempTable('df')\n" +
                                        "df.show()", sub_interpreter= "pyspark")
            result = session.wait_util_finished(result.statement_id)
            self.assertEqual('FINISHED', result.status)
            self.assertEqual(1, len(result.results))
            self.assertTrue(len(result.jobUrls) > 0)
            self.assertEqual('TEXT', result.results[0][0])
        finally:
            session.stop()
