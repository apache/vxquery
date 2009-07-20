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

import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.types.processors.CastProcessor;
import org.apache.vxquery.types.processors.NotCastableCastProcessor;
import org.apache.vxquery.util.Filter;

public final class EmptySequenceType implements ItemType {
    public static final EmptySequenceType INSTANCE = new EmptySequenceType();

    private static final Filter<XDMValue> INSTANCE_OF_FILTER = new Filter<XDMValue>() {
        @Override
        public boolean accept(XDMValue value) throws SystemException {
            return value == null;
        }
    };

    private EmptySequenceType() {
    }

    @Override
    public boolean isAtomicType() {
        return false;
    }

    @Override
    public Filter<XDMValue> createInstanceOfFilter(NameCache nameCache) {
        return INSTANCE_OF_FILTER;
    }

    @Override
    public CastProcessor getCastProcessor(XQType inputBaseType) {
        return NotCastableCastProcessor.INSTANCE_XPST0051;
    }
}