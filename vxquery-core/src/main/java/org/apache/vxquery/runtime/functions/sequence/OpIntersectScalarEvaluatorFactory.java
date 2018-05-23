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
package org.apache.vxquery.runtime.functions.sequence;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.TypedPointables;
import org.apache.vxquery.datamodel.builders.sequence.SequenceBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class OpIntersectScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    class Pair {
        private int rootId, localId;

        Pair(int r, int l) {
            rootId = r;
            localId = l;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + localId;
            result = prime * result + rootId;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Pair other = (Pair) obj;
            if (localId != other.localId)
                return false;
            if (rootId != other.rootId)
                return false;
            return true;
        }
    }

    public OpIntersectScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws HyracksDataException {
        final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        final SequenceBuilder sb = new SequenceBuilder();

        final SequencePointable seqleft = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final SequencePointable seqright = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpleft = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TaggedValuePointable tvpright = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
        final TypedPointables tpleft = new TypedPointables();
        final TypedPointables tpright = new TypedPointables();

        // a set of unique root node ids and local node ids
        Set<Pair> nodes = new HashSet<Pair>();

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {
                try {
                    abvs.reset();
                    sb.reset(abvs);
                    TaggedValuePointable tvp1 = args[0];
                    TaggedValuePointable tvp2 = args[1];

                    if ((tvp1.getTag() != ValueTag.SEQUENCE_TAG && tvp1.getTag() != ValueTag.NODE_TREE_TAG)
                            || (tvp2.getTag() != ValueTag.SEQUENCE_TAG && tvp2.getTag() != ValueTag.NODE_TREE_TAG)) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }

                    // If an operand has one item then it is a node tree
                    // If an operand has more than one item then it is a sequence  
                    // IF an operand has 0 items then it is an empty sequence

                    // Add items from the left operand into the hash map
                    if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp1.getValue(seqleft);
                        for (int i = 0; i < seqleft.getEntryCount(); ++i) {
                            seqleft.getEntry(i, tvpleft);
                            if (tvpleft.getTag() != ValueTag.NODE_TREE_TAG) {
                                throw new SystemException(ErrorCode.XPTY0004);
                            }
                            if (!addItem(tvpleft, tpleft, nodes)) {
                                // TODO: What happens when local node id is -1
                            }
                        }
                    } else {
                        if (!addItem(tvp1, tpleft, nodes)) {
                            // TODO: What happens when local node id is -1
                        }
                    }

                    // Check if node IDs from right operand are in the hash map
                    if (tvp2.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp2.getValue(seqright);
                        for (int i = 0; i < seqright.getEntryCount(); ++i) {
                            seqright.getEntry(i, tvpright);
                            if (tvpright.getTag() != ValueTag.NODE_TREE_TAG) {
                                throw new SystemException(ErrorCode.XPTY0004);
                            }
                            if (checkItem(tvpright, tpright, nodes)) {
                                sb.addItem(tvpright);
                                // TODO
                            }

                        }
                    } else {
                        if (checkItem(tvp2, tpright, nodes)) {
                            sb.addItem(tvp2);
                            // TODO
                        }
                    }

                    sb.finish();
                    result.set(abvs);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001);
                }
            }
        };
    }

    /*
     * Adds item to nodes (hash map)
     * Returns: False if local node id doesn't exist
     *          True if item added successfully
     */
    private boolean addItem(TaggedValuePointable tvp, TypedPointables tp, Set<Pair> nodes) {
        int nodeId = FunctionHelper.getLocalNodeId(tvp, tp);
        int rootNodeId = tp.ntp.getRootNodeId();
        System.out.println("Left Node ID: " + nodeId + " root node id: " + rootNodeId);
        if (nodeId == -1) {
            //TODO
            return false;
        }
        nodes.add(new Pair(rootNodeId, nodeId));
        return true;
    }

    /*
     * Checks if node is in hash map
     * Returns: False if local node id doesn't exist
     *          False if node isn't in hash map
     *          True if node is in hash map
     */
    private boolean checkItem(TaggedValuePointable tvp, TypedPointables tp, Set<Pair> nodes) {
        int nodeId = FunctionHelper.getLocalNodeId(tvp, tp);
        int rootNodeId = tp.ntp.getRootNodeId();

        System.out.println("Right Node ID: " + nodeId + " root node id: " + rootNodeId);
        if (nodeId == -1) {
            // TODO
            return false;
        } else if (nodes.contains(new Pair(rootNodeId, nodeId))) {
            System.out.println("Node Found!");
            return true;
        }
        return false;
    }
}