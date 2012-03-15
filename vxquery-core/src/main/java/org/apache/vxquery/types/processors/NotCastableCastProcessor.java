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
package org.apache.vxquery.types.processors;

import org.apache.vxquery.context.StaticContext;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.exceptions.SystemExceptionFactory;
import org.apache.vxquery.types.XQType;
import org.apache.vxquery.v0datamodel.XDMValue;
import org.apache.vxquery.v0datamodel.atomic.AtomicValueFactory;

public final class NotCastableCastProcessor implements CastProcessor {
    public static final CastProcessor INSTANCE_XPTY0004 = new NotCastableCastProcessor(ErrorCode.XPTY0004);
    public static final CastProcessor INSTANCE_XPST0051 = new NotCastableCastProcessor(ErrorCode.XPST0051);
    public static final CastProcessor INSTANCE_XPST0080 = new NotCastableCastProcessor(ErrorCode.XPST0080);

    private ErrorCode eCode;

    private NotCastableCastProcessor(ErrorCode eCode) {
        this.eCode = eCode;
    }

    @Override
    public XDMValue cast(AtomicValueFactory avf, XDMValue value, SystemExceptionFactory ieFactory,
            StaticContext ctx) throws SystemException {
        throw ieFactory.createException(eCode);
    }

    @Override
    public boolean castable(XDMValue value, StaticContext ctx) {
        return false;
    }

    @Override
    public Boolean castable(XQType type) {
        return Boolean.FALSE;
    }

    @Override
    public ErrorCode getCastFailureErrorCode(XQType type) {
        return eCode;
    }
}