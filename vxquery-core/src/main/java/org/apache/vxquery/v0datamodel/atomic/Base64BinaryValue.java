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
package org.apache.vxquery.v0datamodel.atomic;

import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;

import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class Base64BinaryValue extends AtomicValue {
    private byte[] bytes;

    public Base64BinaryValue(CharSequence value) {
        this(Base64.decodeBase64(value.toString().getBytes()));
    }

    public Base64BinaryValue(byte[] bytes) {
        this(bytes, BuiltinTypeRegistry.XS_BASE64_BINARY);
    }

    public Base64BinaryValue(byte[] bytes, AtomicType type) {
        super(type);
        this.bytes = bytes;
    }

    @Override
    public CharSequence getStringValue() {
        return new String(Base64.encodeBase64(bytes));
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Base64BinaryValue)) {
            return false;
        }
        Base64BinaryValue other = (Base64BinaryValue) o;
        return Arrays.equals(bytes, other.bytes);
    }
}