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
package org.apache.vxquery.runtime.functions.error;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluator;
import org.apache.vxquery.runtime.functions.base.AbstractTaggedValueArgumentScalarEvaluatorFactory;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;

public class FnErrorScalarEvaluatorFactory extends AbstractTaggedValueArgumentScalarEvaluatorFactory {
    private static final long serialVersionUID = 1L;

    public FnErrorScalarEvaluatorFactory(IScalarEvaluatorFactory[] args) {
        super(args);
    }

    @SuppressWarnings("unused")
    @Override
    protected IScalarEvaluator createEvaluator(IHyracksTaskContext ctx, IScalarEvaluator[] args)
            throws AlgebricksException {
        final XSQNamePointable qnamep = (XSQNamePointable) XSQNamePointable.FACTORY.createPointable();
        final SequencePointable seqp = (SequencePointable) SequencePointable.FACTORY.createPointable();
        final UTF8StringPointable urip = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable localnamep = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable prefixp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();
        final UTF8StringPointable descriptionp = (UTF8StringPointable) UTF8StringPointable.FACTORY.createPointable();

        final ByteBufferInputStream bbis = new ByteBufferInputStream();
        final DataInputStream di = new DataInputStream(bbis);

        return new AbstractTaggedValueArgumentScalarEvaluator(args) {
            @Override
            protected void evaluate(TaggedValuePointable[] args, IPointable result) throws SystemException {

                // No parameters.
                if (args.length == 0) {
                    throw new SystemException(ErrorCode.FOER0000);
                }
                String namespaceURI;
                String localPart;

                // Only QName parameter.
                if (args.length == 1) {
                    TaggedValuePointable tvp1 = args[0];
                    if (tvp1.getTag() != ValueTag.XS_QNAME_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp1.getValue(qnamep);
                    qnamep.getUri(urip);
                    qnamep.getLocalName(localnamep);
                    try {
                        namespaceURI = FunctionHelper.getStringFromPointable(urip, bbis, di);

                        localPart = FunctionHelper.getStringFromPointable(localnamep, bbis, di);
                    } catch (IOException e) {
                        throw new SystemException(ErrorCode.FOER0000);
                    }
                    // TODO Update to dynamic error.
                    throw new SystemException(ErrorCode.FOER0000);
                }

                // Only QName, description and optional error-object parameters.
                if (args.length > 1) {
                    TaggedValuePointable tvp1 = args[0];
                    if (tvp1.getTag() == ValueTag.SEQUENCE_TAG) {
                        tvp1.getValue(seqp);
                        if (seqp.getEntryCount() == 0) {
                            namespaceURI = "http://www.w3.org/2005/xqt-errors";
                            localPart = "FOER0000";
                        } else {
                            throw new SystemException(ErrorCode.FORG0006);
                        }
                    } else if (tvp1.getTag() == ValueTag.XS_QNAME_TAG) {
                        tvp1.getValue(qnamep);
                        qnamep.getUri(urip);
                        qnamep.getLocalName(localnamep);
                        try {
                            namespaceURI = FunctionHelper.getStringFromPointable(urip, bbis, di);

                            localPart = FunctionHelper.getStringFromPointable(localnamep, bbis, di);
                        } catch (IOException e) {
                            throw new SystemException(ErrorCode.FORG0006);
                        }
                    } else {
                        throw new SystemException(ErrorCode.FORG0006);
                    }

                    TaggedValuePointable tvp2 = args[1];
                    if (tvp2.getTag() != ValueTag.XS_STRING_TAG) {
                        throw new SystemException(ErrorCode.FORG0006);
                    }
                    tvp2.getValue(descriptionp);

                    if (args.length == 3) {
                        TaggedValuePointable tvp3 = args[2];
                        // TODO do something with last parameter.
                    }

                    // TODO Update to dynamic error.
                    throw new SystemException(ErrorCode.FOER0000);
                }
            }
        };
    }
}
