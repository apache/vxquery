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
package org.apache.vxquery.types;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.exceptions.SystemExceptionFactory;
import org.apache.vxquery.types.processors.CastProcessor;
import org.apache.vxquery.types.processors.NotCastableCastProcessor;
import org.apache.vxquery.util.Filter;

public class QuantifiedType implements XQType {
    private XQType contentType;
    private Quantifier quantifier;

    public QuantifiedType(XQType contentType, Quantifier quantifier) {
        this.contentType = contentType;
        this.quantifier = quantifier;
    }

    public XQType getContentType() {
        return contentType;
    }

    public Quantifier getQuantifier() {
        return quantifier;
    }

    @Override
    public Filter<XDMValue> createInstanceOfFilter() {
        return null;
    }

    @Override
    public CastProcessor getCastProcessor(XQType inputBaseType) {
        final CastProcessor contentTypeProcessor = contentType.getCastProcessor(inputBaseType);
        switch (quantifier) {
            case QUANT_ONE:
            case QUANT_PLUS:
                return contentTypeProcessor;

            case QUANT_QUESTION:
            case QUANT_STAR:
                return new CastProcessor() {
                    @Override
                    public XDMValue cast(AtomicValueFactory avf, XDMValue value, SystemExceptionFactory ieFactory,
                            StaticContext ctx) throws SystemException {
                        if (value == null) {
                            return null;
                        }
                        return contentTypeProcessor.cast(avf, value, ieFactory, ctx);
                    }

                    @Override
                    public boolean castable(XDMValue value, StaticContext ctx) {
                        if (value == null) {
                            return true;
                        }
                        return contentTypeProcessor.castable(value, ctx);
                    }

                    @Override
                    public Boolean castable(XQType type) {
                        return contentTypeProcessor.castable(type);
                    }

                    @Override
                    public ErrorCode getCastFailureErrorCode(XQType type) {
                        return contentTypeProcessor.getCastFailureErrorCode(type);
                    }
                };
        }
        return NotCastableCastProcessor.INSTANCE_XPST0051;
    }
}