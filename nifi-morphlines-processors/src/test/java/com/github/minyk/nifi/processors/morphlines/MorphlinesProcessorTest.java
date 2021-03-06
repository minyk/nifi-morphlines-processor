/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.minyk.nifi.processors.morphlines;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;


public class MorphlinesProcessorTest {

    private TestRunner testRunner;
    private URL data;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(MorphlinesProcessor.class);
        URL file = MorphlinesProcessorTest.class.getClassLoader().getResource("morphline_with_exception.conf");
        testRunner.setProperty(MorphlinesProcessor.MORPHLINES_FILE,file.getPath());
        testRunner.setProperty(MorphlinesProcessor.MORPHLINES_ID, "morphline1");
        //testRunner.setRelationshipUnavailable(MorphlinesProcessor.REL_FAILURE);

        data = MorphlinesProcessorTest.class.getClassLoader().getResource("records.txt");
    }

    @Test
    public void testProcessorSuccess() throws IOException {
        try (
                InputStream inputStream = getClass().getResourceAsStream("/records.txt")
        ) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "records.txt");
            testRunner.enqueue(inputStream, attributes);
            testRunner.run();
        }
        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(MorphlinesProcessor.REL_SUCCESS);

        assertEquals(1, result.size());
    }

    @Test
    public void testProcessorFail() throws IOException {
        try (
                InputStream inputStream = getClass().getResourceAsStream("/failrecords.txt")
        ) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), "failrecords.txt");
            testRunner.enqueue(inputStream, attributes);
            testRunner.run();
        }
        List<MockFlowFile> result = testRunner.getFlowFilesForRelationship(MorphlinesProcessor.REL_FAILURE);

        assertEquals(1, result.size());
    }
}
