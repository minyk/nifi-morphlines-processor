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

import com.google.common.base.Preconditions;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@Tags({"kitesdk", "morphlines"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class MyProcessor extends AbstractProcessor {

    private static Command morphline;
    private final Record record = new Record();
    private static final Collector collector = new Collector();

    public static final PropertyDescriptor MORPHLINES_ID = new PropertyDescriptor
            .Builder().name("Morphlines ID")
            .description("Identifier of the morphlines context")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MORPHLINES_FILE = new PropertyDescriptor
            .Builder().name("Morphlines File")
            .description("File for the morphlines context")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Example relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Example relationship")
            .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MORPHLINES_FILE);
        descriptors.add(MORPHLINES_ID);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = session.get();
        if ( inputFlowFile == null ) {
            return;
        }

        getLogger().debug("morphlines processor triggered.");

        final byte[] value =new byte[(int) inputFlowFile.getSize()];
        session.read(inputFlowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, value);
            }
        });

        record.put(Fields.ATTACHMENT_BODY, value);
        collector.reset();
        boolean success = getMorphlinesCommand(context).process(record);

        if(success) {

            FlowFile outputFlowFile = session.create();
            outputFlowFile = session.append(outputFlowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(collector.getRecords().get(0).getFirstValue("value").toString().getBytes());
                }
            });

            getLogger().debug("Move result to success connection.");
            session.transfer(outputFlowFile, REL_SUCCESS);
        } else {
            getLogger().warn("Fail to process morphlines record.");
            getLogger().debug("Move result to success connection.");
            session.transfer(session.penalize(inputFlowFile), REL_FAILURE);
        }

        record.getFields().clear();
        session.remove(inputFlowFile);
    }

    private static Command getMorphlinesCommand(final ProcessContext context) {
        if(morphline != null) {
            return morphline;
        } else {
            File morphLineFile = new File(context.getProperty(MORPHLINES_FILE).getValue());
            String morphLinesId = context.getProperty(MORPHLINES_ID).getValue();
            MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
            morphline = new org.kitesdk.morphline.base.Compiler().compile(morphLineFile, morphLinesId, morphlineContext, collector);
            return morphline;
        }
    }

    private static final class Collector implements Command {

        private final List<Record> results = new ArrayList();

        public List<Record> getRecords() {
            return results;
        }

        public void reset() {
            results.clear();
        }

        @Override
        public Command getParent() {
            return null;
        }

        @Override
        public void notify(Record notification) {
        }

        @Override
        public boolean process(Record record) {
            Preconditions.checkNotNull(record);
            results.add(record);
            return true;
        }

    }
}
