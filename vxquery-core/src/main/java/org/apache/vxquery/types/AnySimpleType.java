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

public final class AnySimpleType implements SimpleType {
    public static final SimpleType INSTANCE = new AnySimpleType();

    private AnySimpleType() {
    }

    @Override
    public int getTypeId() {
        return BuiltinTypeConstants.XS_ANY_SIMPLE_TYPE_ID;
    }

    @Override
    public SchemaType getBaseType() {
        return AnyType.INSTANCE;
    }

    @Override
    public DerivationProcess getDerivationProcess() {
        return DerivationProcess.RESTRICTION;
    }

    @Override
    public boolean isAtomicType() {
        return false;
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
        return String.valueOf(BuiltinTypeRegistry.INSTANCE.getTypeName(getTypeId()));
    }

    @Override
    public int hashCode() {
        return AnySimpleType.class.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof AnySimpleType;
    }
}
