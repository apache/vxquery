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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.vxquery.context.DynamicContext;
import org.apache.vxquery.hdfs2.HDFSFunctions;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.apache.vxquery.xmlparser.XMLParser;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class VXQueryCollectionOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;
    private short dataSourceId;
    private short totalDataSources;
    private String[] collectionPartitions;
    private List<Integer> childSeq;
    protected static final Logger LOGGER = Logger.getLogger(VXQueryCollectionOperatorDescriptor.class.getName());
    
    public VXQueryCollectionOperatorDescriptor(IOperatorDescriptorRegistry spec, VXQueryCollectionDataSource ds,
            RecordDescriptor rDesc) {
        super(spec, 1, 1);
        collectionPartitions = ds.getPartitions();
        dataSourceId = (short) ds.getDataSourceId();
        totalDataSources = (short) ds.getTotalDataSources();
        childSeq = ds.getChildSeq();
        recordDescriptors[0] = rDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        final FrameTupleAccessor fta = new FrameTupleAccessor(ctx.getFrameSize(),
                recordDescProvider.getInputRecordDescriptor(getActivityId(), 0));
        final int fieldOutputCount = recordDescProvider.getOutputRecordDescriptor(getActivityId(), 0).getFieldCount();
        final ByteBuffer frame = ctx.allocateFrame();
        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize(), fieldOutputCount);
        final short partitionId = (short) ctx.getTaskAttemptId().getTaskId().getPartition();
        final ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider(partitionId, dataSourceId, totalDataSources);
        final String nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        final DynamicContext dCtx = (DynamicContext) ctx.getJobletContext().getGlobalJobData();

        final String collectionName = collectionPartitions[partition % collectionPartitions.length];
        final XMLParser parser = new XMLParser(false, nodeIdProvider, nodeId, frame, appender, childSeq,
                dCtx.getStaticContext());

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
                appender.reset(frame, true);
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                fta.reset(buffer);
                String collectionModifiedName = collectionName.replace("${nodeId}", nodeId);
                
                File collectionDirectory = new File(collectionModifiedName);
                //check if directory is in the local file system
                if(collectionDirectory.exists())
                {
                	System.out.println("searching in local for file : " + collectionDirectory.getName());
	                // Go through each tuple.
	                if (collectionDirectory.isDirectory()) {
	                    for (int tupleIndex = 0; tupleIndex < fta.getTupleCount(); ++tupleIndex) {
	                        @SuppressWarnings("unchecked")
	                        Iterator<File> it = FileUtils.iterateFiles(collectionDirectory, new VXQueryIOFileFilter(),
	                                TrueFileFilter.INSTANCE);
	                        while (it.hasNext()) {
	                            File xmlDocument = it.next();
	                            if (LOGGER.isLoggable(Level.FINE)) {
	                                LOGGER.fine("Starting to read XML document: " + xmlDocument.getAbsolutePath());
	                            }
	                            parser.parseElements(xmlDocument, writer, fta, tupleIndex);
	                        }
	                    }
	                } else {
	                    throw new HyracksDataException("Invalid directory parameter (" + nodeId + ":"
	                            + collectionDirectory.getAbsolutePath() + ") passed to collection.");
	                }
                }
                //else check in HDFS file system
                else
                {
                	System.out.println("searching in hdfs for directory : " + collectionDirectory.getName());
                	HDFSFunctions hdfs = new HDFSFunctions();
                	FileSystem fs = hdfs.getFileSystem();
                	if (fs != null)
                	{
	                	Path directory = new Path(collectionModifiedName);
	                	Path xmlDocument;
	                	try {
							if (fs.exists(directory) && fs.isDirectory(directory))
							{
								for (int tupleIndex = 0; tupleIndex < fta.getTupleCount(); ++tupleIndex) {
								//read directory files from HDFS
								RemoteIterator<LocatedFileStatus> it = fs.listFiles(directory, true);
							    while (it.hasNext())
							    {
							    	xmlDocument = it.next().getPath();
							        if (fs.isFile(xmlDocument))
							        {
							        	if (LOGGER.isLoggable(Level.FINE)) {
							                LOGGER.fine("Starting to read XML document: " + xmlDocument.getName());
							            }
							        	InputStream in = fs.open(xmlDocument).getWrappedStream();
							        	parser.parseHDFSElements(in, writer, fta, tupleIndex);
							        }
							    }
								}
							}
							else
							{
								 throw new HyracksDataException("Invalid directory parameter (" + nodeId + ":"
							            + collectionDirectory.getAbsolutePath() + ") passed to collection.");
							}
						} catch (FileNotFoundException e) {
							System.err.println(e);
						} catch (IOException e) {
							System.err.println(e);
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
                fta.reset(frame);
                if (fta.getTupleCount() > 0) {
                    FrameUtils.flushFrame(frame, writer);
                }
                writer.close();
            }
        };
    }
}