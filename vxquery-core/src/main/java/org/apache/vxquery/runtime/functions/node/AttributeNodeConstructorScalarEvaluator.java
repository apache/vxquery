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

import java.io.IOException;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.builders.nodes.AttributeNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class AttributeNodeConstructorScalarEvaluator extends AbstractNodeConstructorScalarEvaluator {
    private final AttributeNodeBuilder anb;

    private final XSQNamePointable namep;

    private final UTF8StringPointable strp;

    public AttributeNodeConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        anb = new AttributeNodeBuilder();
        namep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
        strp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
    }

    @Override
    protected void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException {
        anb.reset(mvs);
        TaggedValuePointable nameArg = args[0];
        if (nameArg.getTag() != ValueTag.XS_QNAME_TAG) {
            throw new SystemException(ErrorCode.XPST0081);
        }
        nameArg.getValue(namep);
        namep.getUri(strp);
        int uriCode = db.lookup(strp);
        namep.getPrefix(strp);
        int prefixCode = db.lookup(strp);
        namep.getLocalName(strp);
        int localCode = db.lookup(strp);
        anb.setName(uriCode, localCode, prefixCode);
        TaggedValuePointable valueArg = args[1];
        anb.setValue(valueArg);
        anb.finish();
    }

    @Override
    protected boolean createsDictionary() {
        return true;
    }
}
