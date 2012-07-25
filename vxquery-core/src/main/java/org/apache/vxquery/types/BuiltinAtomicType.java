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

final class BuiltinAtomicType implements AtomicType {
    private final int id;
    private final SchemaType baseType;
    private final DerivationProcess derivationProcess;

    BuiltinAtomicType(int id, SimpleType baseType, DerivationProcess derivationProcess) {
        this.id = id;
        this.baseType = baseType;
        this.derivationProcess = derivationProcess;
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
    public String toString() {
        return String.valueOf(BuiltinTypeRegistry.INSTANCE.getTypeName(id));
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BuiltinAtomicType other = (BuiltinAtomicType) obj;
        if (id != other.id)
            return false;
        return true;
    }
}