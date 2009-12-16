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

import org.apache.vxquery.datamodel.DMOKind;
import org.apache.vxquery.datamodel.XDMAtomicValue;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.types.processors.CastProcessor;
import org.apache.vxquery.util.Filter;

final class BuiltinAtomicType implements AtomicType {
    private final int id;
    private final SchemaType baseType;
    private final DerivationProcess derivationProcess;
    private final CastProcessor castProcessor;
    private final Filter<XDMValue> instanceFilter;

    BuiltinAtomicType(int id, SimpleType baseType, DerivationProcess derivationProcess, CastProcessor castProcessor) {
        this.id = id;
        this.baseType = baseType;
        this.derivationProcess = derivationProcess;
        this.castProcessor = castProcessor;
        instanceFilter = new Filter<XDMValue>() {
            @Override
            public boolean accept(XDMValue value) throws SystemException {
                if (value.getDMOKind() != DMOKind.ATOMIC_VALUE) {
                    return false;
                }
                XDMAtomicValue av = (XDMAtomicValue) value;
                AtomicType at = av.getAtomicType();
                return TypeUtils.isSubtypeTypeOf(at, BuiltinAtomicType.this);
            }
        };
    }

    @Override
    public boolean isAtomicType() {
        return true;
    }

    @Override
    public int getTypeId() {
        return id;
    }

    @Override
    public SchemaType getBaseType() {
        return baseType;
    }

    @Override
    public DerivationProcess getDerivationProcess() {
        return derivationProcess;
    }

    @Override
    public boolean isComplexType() {
        return false;
    }

    @Override
    public boolean isSimpleType() {
        return true;
    }

    @Override
    public Filter<XDMValue> createInstanceOfFilter() {
        return instanceFilter;
    }

    @Override
    public CastProcessor getCastProcessor(XQType inputBaseType) {
        return castProcessor;
    }

    @Override
    public String toString() {
        return String.valueOf(BuiltinTypeRegistry.INSTANCE.getTypeName(id));
    }
}