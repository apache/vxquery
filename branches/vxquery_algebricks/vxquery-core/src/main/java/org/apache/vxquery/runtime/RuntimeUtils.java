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
package org.apache.vxquery.runtime;

import org.apache.vxquery.datamodel.XDMItem;
import org.apache.vxquery.datamodel.atomic.AtomicValueFactory;
import org.apache.vxquery.datamodel.atomic.DoubleValue;
import org.apache.vxquery.datamodel.atomic.NumericValue;
import org.apache.vxquery.datamodel.atomic.UntypedAtomicValue;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.base.CloseableIterator;
import org.apache.vxquery.runtime.base.RuntimeIterator;
import org.apache.vxquery.runtime.core.CloseableIteratorAdapter;

public final class RuntimeUtils {
    private RuntimeUtils() {
    }

    public static XDMItem fetchItemEagerly(RuntimeIterator iterator, CallStackFrame frame) throws SystemException {
        Object o = iterator.evaluateEagerly(frame);
        if (o != null && !(o instanceof XDMItem)) {
            throw new SystemException(ErrorCode.XPTY0004);
        }
        return (XDMItem) o;
    }

    public static NumericValue fetchNumericItemEagerly(RuntimeIterator iterator, CallStackFrame frame)
            throws SystemException {
        Object o = iterator.evaluateEagerly(frame);
        if (o != null) {
            if (o instanceof UntypedAtomicValue) {
                o = castUntypedAtomicToNumeric(frame.getRuntimeControlBlock().getAtomicValueFactory(),
                        (UntypedAtomicValue) o);
            } else if (!(o instanceof NumericValue)) {
                throw new SystemException(ErrorCode.XPTY0004);
            }
        }
        return (NumericValue) o;
    }

    private static DoubleValue castUntypedAtomicToNumeric(AtomicValueFactory avf, UntypedAtomicValue uav)
            throws SystemException {
        try {
            double d = Double.parseDouble(String.valueOf(uav.getStringValue()));
            return avf.createDouble(d);
        } catch (NumberFormatException e) {
            throw new SystemException(ErrorCode.FORG0001);
        }
    }

    public static CloseableIterator createCloseableIterator(CallStackFrame frame, RuntimeIterator ri) {
        return new CloseableIteratorAdapter(frame, ri);
    }
}