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
package org.apache.vxquery.runtime.functions.index;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.index.updateIndex.IndexUpdater;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;

import javax.xml.bind.JAXBException;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Delete the index of a given index directory
 */
public class IndexDeleteEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    public IndexDeleteEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final UTF8StringPointable stringp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final TaggedValuePointable nodep = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final ByteBufferInputStream bbis = new ByteBufferInputStream();
        final DataInputStream di = new DataInputStream(bbis);
        final SequenceBuilder sb = new SequenceBuilder();
        final ArrayBackedValueStorage abvsFileNode = new ArrayBackedValueStorage();
        final int partition = ctx.getTaskAttemptId().getTaskId().getPartition();
        final String nodeId = ctx.getJobletContext().getApplicationContext().getNodeId();
        final ITreeNodeIdProvider nodeIdProvider = new TreeNodeIdProvider((short) partition);

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {

            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                IndexUpdater updater = new IndexUpdater(args, result, stringp, bbis, di, sb, abvs, nodeIdProvider,
                        abvsFileNode, nodep, nodeId);
                try {
                    updater.setup();
                    updater.deleteAllIndexes();
                    XDMConstants.setTrue(result);
                } catch (IOException | NoSuchAlgorithmException | JAXBException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }

            }

        };
    }
}
