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
package org.apache.vxquery.runtime.functions.node;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.xmlparser.ITreeNodeIdProvider;
import org.apache.vxquery.xmlparser.TreeNodeIdProvider;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractNodeConstructorScalarEvaluator extends AbstractTaggedValueArgumentScalarEvaluator {
    protected final static ITreeNodeIdProvider NodeConstructorIdProvider = new TreeNodeIdProvider((short) 0);

    protected final IHyracksTaskContext ctx;

    private final ArrayBackedValueStorage abvs;

    private final DictionaryBuilder db;

    private final ArrayBackedValueStorage contentAbvs;

    protected final ITreeNodeIdProvider nodeIdProvider;

    public AbstractNodeConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(args);
        this.ctx = ctx;
        abvs = new ArrayBackedValueStorage();
        db = createsDictionary() ? new DictionaryBuilder() : null;
        contentAbvs = createsDictionary() ? new ArrayBackedValueStorage() : abvs;
        if (createsNodeId()) {
            nodeIdProvider = NodeConstructorIdProvider;
        } else {
            nodeIdProvider = null;
        }
    }

    @Override
    protected final void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
        abvs.reset();
        contentAbvs.reset();
        try {
            DataOutput mainOut = abvs.getDataOutput();
            mainOut.write(ValueTag.NODE_TREE_TAG);
            byte header = (byte) (createsDictionary() ? NodeTreePointable.HEADER_DICTIONARY_EXISTS_MASK : 0);

            header |= (byte) (nodeIdProvider != null ? NodeTreePointable.HEADER_NODEID_EXISTS_MASK : 0);
            mainOut.write(header);

            if (nodeIdProvider != null) {
                mainOut.writeInt(nodeIdProvider.getId());
            }
            constructNode(db, args, contentAbvs);
            if (createsDictionary()) {
                db.write(abvs);
                abvs.append(contentAbvs);
            }
            result.set(abvs);
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

    protected abstract void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException;

    protected abstract boolean createsDictionary();

    protected abstract boolean createsNodeId();
}
