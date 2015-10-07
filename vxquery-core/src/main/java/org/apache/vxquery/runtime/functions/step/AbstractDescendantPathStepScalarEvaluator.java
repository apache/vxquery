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
package org.apache.vxquery.runtime.functions.step;

import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;

public abstract class AbstractDescendantPathStepScalarEvaluator extends AbstractPathStepScalarEvaluator {
    private final DocumentNodePointable dnp;

    private final ElementNodePointable enp;

    public AbstractDescendantPathStepScalarEvaluator(IScalarEvaluator[] args, IHyracksTaskContext ctx) {
        super(args, ctx);
        dnp = (DocumentNodePointable) DocumentNodePointable.FACTORY.createPointable();
        enp = (ElementNodePointable) ElementNodePointable.FACTORY.createPointable();
    }

    @Override
    protected abstract void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException;

    /**
     * Search through all tree children and children's children.
     * 
     * @param nodePointable
     * @throws SystemException
     */
    protected void searchSubtree(TaggedValuePointable nodePointable) throws SystemException {
        try {
            SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
            boolean search = false;

            // Find all child element to search.
            switch (nodePointable.getTag()) {
                case ValueTag.DOCUMENT_NODE_TAG:
                    nodePointable.getValue(dnp);
                    dnp.getContent(ntp, seqp);
                    search = true;
                    break;

                case ValueTag.ELEMENT_NODE_TAG:
                    nodePointable.getValue(enp);
                    if (enp.childrenChunkExists()) {
                        enp.getChildrenSequence(ntp, seqp);
                        search = true;
                    }
                    break;
            }

            if (search) {
                int seqSize = seqp.getEntryCount();
                for (int i = 0; i < seqSize; ++i) {
                    seqp.getEntry(i, itemTvp);
                    // Only search element nodes.
                    if (itemTvp.getTag() == ValueTag.ELEMENT_NODE_TAG) {
                        appendNodeToResult();
                        // Now check this elements children.
                        TaggedValuePointable tvpTemp = (TaggedValuePointable) TaggedValuePointable.FACTORY
                                .createPointable();
                        tvpTemp.set(itemTvp);
                        searchSubtree(tvpTemp);
                    }
                }
            }
        } catch (IOException e) {
            throw new SystemException(ErrorCode.SYSE0001, e);
        }
    }

}
