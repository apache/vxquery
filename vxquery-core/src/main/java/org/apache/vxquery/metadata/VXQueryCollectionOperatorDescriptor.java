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
package org.apache.vxquery.metadata;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.Inet4Address;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameFieldAppender;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.hdfs.ContextFactory;
import org.apache.hyracks.hdfs2.dataflow.FileSplitsFactory;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.hdfs2.HDFSFunctions;
import org.apache.vxquery.jsonparser.JSONParser;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.apache.vxquery.xmlparser.XMLParser;

public class VXQueryCollectionOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private short dataSourceId;
    private short totalDataSources;
    private String[] collectionPartitions;
    private List<Integer> childSeq;
    private List<Byte[]> valueSeq;
    protected static final Logger LOGGER = Logger.getLogger(VXQueryCollectionOperatorDescriptor.class.getName());
    private HDFSFunctions hdfs;
    private String tag;
    private static final String START_TAG = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    private final String hdfsConf;
    private final Map<String, NodeControllerInfo> nodeControllerInfos;

    public VXQueryCollectionOperatorDescriptor(IOperatorDescriptorRegistry spec, AbstractVXQueryDataSource ds,
            RecordDescriptor rDesc, String hdfsConf, Map<String, NodeControllerInfo> nodeControllerInfos) {
        super(spec, 1, 1);
        collectionPartitions = ds.getPartitions();
        dataSourceId = (short) ds.getDataSourceId();
        totalDataSources = (short) ds.getTotalDataSources();
        childSeq = ds.getChildSeq();
        valueSeq = ds.getValueSeq();
        outRecDescs[0] = rDesc;
        this.tag = ds.getTag();
        this.hdfsConf = hdfsConf;
        this.nodeControllerInfos = nodeControllerInfos;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final FrameTupleAccessor fta = new FrameTupleAccessor(
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
        final int fieldOutputCount = recordDescProvider.getOutputRecordDescriptor(getActivityId(), 0).getFieldCount();
        final IFrame frame = new VSizeFrame(ctx);
        final IFrameFieldAppender appender = new FrameFixedFieldTupleAppender(fieldOutputCount);
        final short partitionId = (short) ctx.getTaskAttemptId().getTaskId().getPartition();
        final ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider(partitionId, dataSourceId, totalDataSources);
        final String nodeId = ctx.getJobletContext().getServiceContext().getNodeId();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();
        final ArrayBackedValueStorage jsonAbvs = new ArrayBackedValueStorage();
        final String collectionName = collectionPartitions[partition % collectionPartitions.length];
        final XMLParser parser = new XMLParser(false, nodeIdProvider, nodeId, appender, childSeq,
                dCtx.getStaticContext());
        final JSONParser jparser = new JSONParser(valueSeq);

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
                appender.reset(frame, true);
                writer.open();
                hdfs = new HDFSFunctions(nodeControllerInfos, hdfsConf);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                fta.reset(buffer);
                String collectionModifiedName = collectionName.replace("${nodeId}", nodeId);

                if (!collectionModifiedName.contains("hdfs:/")) {
                    File collectionDirectory = new File(collectionModifiedName);
                    // check if directory is in the local file system
                    if (collectionDirectory.exists()) {
                        // Go through each tuple.
                        if (collectionDirectory.isDirectory()) {
                            xmlAndJsonCollection(collectionDirectory);
                        } else {
                            throw new HyracksDataException("Invalid directory parameter (" + nodeId + ":"
                                    + collectionDirectory.getAbsolutePath() + ") passed to collection.");
                        }
                    }
                } else {
                    // Else check in HDFS file system
                    // Get instance of the HDFS filesystem
                    FileSystem fs = hdfs.getFileSystem();
                    if (fs != null) {
                        collectionModifiedName = collectionModifiedName.replaceAll("hdfs:/", "");
                        Path directory = new Path(collectionModifiedName);
                        Path xmlDocument;
                        if (tag != null) {
                            hdfs.setJob(directory.toString(), tag);
                            tag = "<" + tag + ">";
                            Job job = hdfs.getJob();
                            InputFormat inputFormat = hdfs.getinputFormat();
                            try {
                                hdfs.scheduleSplits();
                                ArrayList<Integer> schedule = hdfs
                                        .getScheduleForNode(Inet4Address.getLoopbackAddress().getHostAddress());
                                List<InputSplit> splits = hdfs.getSplits();
                                List<FileSplit> fileSplits = new ArrayList<>();
                                for (int i : schedule) {
                                    fileSplits.add((FileSplit) splits.get(i));
                                }
                                FileSplitsFactory splitsFactory = new FileSplitsFactory(fileSplits);
                                List<FileSplit> inputSplits = splitsFactory.getSplits();
                                ContextFactory ctxFactory = new ContextFactory();
                                int size = inputSplits.size();
                                InputStream stream;
                                String value;
                                RecordReader reader;
                                TaskAttemptContext context;
                                for (int i = 0; i < size; i++) {
                                    // read split
                                    context = ctxFactory.createContext(job.getConfiguration(), i);
                                    reader = inputFormat.createRecordReader(inputSplits.get(i), context);
                                    reader.initialize(inputSplits.get(i), context);
                                    while (reader.nextKeyValue()) {
                                        value = reader.getCurrentValue().toString();
                                        // Split value if it contains more than
                                        // one item with the tag
                                        if (StringUtils.countMatches(value, tag) > 1) {
                                            String[] items = value.split(tag);
                                            for (String item : items) {
                                                if (item.length() > 0) {
                                                    item = START_TAG + tag + item;
                                                    stream = new ByteArrayInputStream(
                                                            item.getBytes(StandardCharsets.UTF_8));
                                                    parser.parseHDFSElements(stream, writer, fta, i);
                                                    stream.close();
                                                }
                                            }
                                        } else {
                                            value = START_TAG + value;
                                            // create an input stream to the
                                            // file currently reading and send
                                            // it to parser
                                            stream = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
                                            parser.parseHDFSElements(stream, writer, fta, i);
                                            stream.close();
                                        }
                                    }
                                    reader.close();
                                }

                            } catch (Exception e) {
                                throw new HyracksDataException(e);
                            }
                        } else {
                            try {
                                // check if the path exists and is a directory
                                if (fs.exists(directory) && fs.isDirectory(directory)) {
                                    for (int tupleIndex = 0; tupleIndex < fta.getTupleCount(); ++tupleIndex) {
                                        // read every file in the directory
                                        RemoteIterator<LocatedFileStatus> it = fs.listFiles(directory, true);
                                        while (it.hasNext()) {
                                            xmlDocument = it.next().getPath();
                                            if (fs.isFile(xmlDocument)) {
                                                if (LOGGER.isLoggable(Level.FINE)) {
                                                    LOGGER.fine(
                                                            "Starting to read XML document: " + xmlDocument.getName());
                                                }
                                                // create an input stream to the
                                                // file currently reading and
                                                // send it to parser
                                                InputStream in = fs.open(xmlDocument).getWrappedStream();
                                                parser.parseHDFSElements(in, writer, fta, tupleIndex);
                                                in.close();
                                            }
                                        }
                                    }
                                } else {
                                    throw new HyracksDataException("Invalid HDFS directory parameter (" + nodeId + ":"
                                            + directory + ") passed to collection.");
                                }
                            } catch (Exception e) {
                                throw new HyracksDataException(e);
                            }
                        }
                        try {
                            fs.close();
                        } catch (Exception e) {
                            throw new HyracksDataException(e);
                        }
                    }
                }
            }

            public void xmlAndJsonCollection(File directory) throws HyracksDataException {
                Reader input;
                for (int tupleIndex = 0; tupleIndex < fta.getTupleCount(); ++tupleIndex) {
                    Iterator<File> it = FileUtils.iterateFiles(directory, new VXQueryIOFileFilter(),
                            TrueFileFilter.INSTANCE);
                    while (it.hasNext()) {
                        File file = it.next();
                        String fileName = file.getName().toLowerCase();
                        if (fileName.endsWith(".xml")) {
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("Starting to read XML document: " + file.getAbsolutePath());
                            }
                            parser.parseElements(file, writer, tupleIndex);
                        } else if (fileName.endsWith(".json")) {
                            if (LOGGER.isLoggable(Level.FINE)) {
                                LOGGER.fine("Starting to read JSON document: " + file.getAbsolutePath());
                            }
                            try {
                                jsonAbvs.reset();
                                input = new InputStreamReader(new FileInputStream(file));
                                jparser.parse(input, jsonAbvs, writer, appender);
                            } catch (FileNotFoundException e) {
                                throw new HyracksDataException(e.toString());
                            }
                        }
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                writer.fail();
            }

            @Override
            public void close() throws HyracksDataException {
                // Check if needed?
                if (appender.getTupleCount() > 0) {
                    appender.flush(writer);
                }
                writer.close();
            }
        };
    }
}
