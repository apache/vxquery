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
import org.apache.vxquery.datamodel.builders.nodes.CommentNodeBuilder;
import org.apache.vxquery.datamodel.builders.nodes.DictionaryBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;

import edu.uci.ics.hyracks.algebricks.runtime.base.IScalarEvaluator;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.primitive.VoidPointable;

public class CommentNodeConstructorScalarEvaluator extends AbstractNodeConstructorScalarEvaluator {
    private final CommentNodeBuilder cnb;

    private final VoidPointable vp;

    public CommentNodeConstructorScalarEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args) {
        super(ctx, args);
        cnb = new CommentNodeBuilder();
        vp = (VoidPointable) VoidPointable.FACTORY.createPointable();
    }

    @Override
    protected void constructNode(DictionaryBuilder db, TaggedValuePointable[] args, IMutableValueStorage mvs)
            throws IOException, SystemException {
        cnb.reset(mvs);
        TaggedValuePointable arg = args[0];
        if (arg.getTag() != ValueTag.XS_UNTYPED_ATOMIC_TAG && arg.getTag() != ValueTag.XS_STRING_TAG) {
            throw new SystemException(ErrorCode.TODO);
        }
        arg.getValue(vp);
        cnb.setValue(vp);
        cnb.finish();
    }

    @Override
    protected boolean createsDictionary() {
        return false;
    }
}