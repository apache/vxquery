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
package org.apache.vxquery.runtime.functions;

import java.io.IOException;
import java.io.InputStream;

import org.xml.sax.InputSource;

import org.apache.vxquery.context.DocumentURIResolver;
import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.XDMAtomicValue;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.functions.Function;
import org.apache.vxquery.runtime.CallStackFrame;
import org.apache.vxquery.runtime.RegisterAllocator;
import org.apache.vxquery.runtime.RuntimeControlBlock;
import org.apache.vxquery.runtime.base.AbstractEagerlyEvaluatedFunctionIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.runtime.util.XMLParserUtils;

public class FnDocIterator extends AbstractEagerlyEvaluatedFunctionIterator {
    public FnDocIterator(RegisterAllocator rAllocator, Function fn, RuntimeIterator[] arguments, StaticContext ctx) {
        super(rAllocator, fn, arguments, ctx);
    }

    @Override
    public Object evaluateEagerly(CallStackFrame frame) throws SystemException {
        XDMAtomicValue uriValue = (XDMAtomicValue) arguments[0].evaluateEagerly(frame);
        RuntimeControlBlock rcb = frame.getRuntimeControlBlock();
        String uri = uriValue.getStringValue().toString();
        DocumentURIResolver docUriResolver = rcb.getDynamicContext().getStaticContext().getDataspaceContext()
                .getDocumentURIResolver();
        InputStream in;
        try {
            in = docUriResolver.resolveDocumentURI(uri, rcb.getDynamicContext().getStaticContext().getBaseUri());
        } catch (IOException e) {
            throw new SystemException(ErrorCode.FODC0002, e, uri);
        }
        return XMLParserUtils.parseInputSource(rcb, new InputSource(in));
    }
}