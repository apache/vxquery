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

import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class HexBinaryValue extends AtomicValue {
    private byte[] bytes;

    public HexBinaryValue(CharSequence value) throws SystemException {
        this(decode(value));
    }

    private static byte[] decode(CharSequence value) throws SystemException {
        int len = value.length();
        int blen = (len / 2) + (len % 2);
        byte[] bytes = new byte[blen];
        int idx = 0;
        if ((len & 1) == 1) {
            char ch = value.charAt(0);
            bytes[0] = hexByte(ch);
            idx++;
        }
        for (; idx < len; idx += 2) {
            bytes[(idx / 2) + (idx & 1)] = (byte) (((int) hexByte(value.charAt(idx))) * 16 + ((int) hexByte(value
                    .charAt(idx + 1))));
        }
        return bytes;
    }

    private static byte hexByte(char ch) throws SystemException {
        if (ch >= '0' && ch <= '9') {
            return (byte) (ch - '0');
        }
        ch = Character.toUpperCase(ch);
        if (ch >= 'A' && ch <= 'F') {
            return (byte) (ch - 'A' + 10);
        }
        throw new SystemException(ErrorCode.FORG0001);
    }

    public HexBinaryValue(byte[] bytes) {
        this(bytes, BuiltinTypeRegistry.XS_HEX_BINARY);
    }

    public HexBinaryValue(byte[] bytes, AtomicType type) {
        super(type);
        this.bytes = bytes;
    }

    @Override
    public CharSequence getStringValue() {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < bytes.length; ++i) {
            buffer.append(Integer.toHexString(bytes[i]));
        }
        return buffer;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof HexBinaryValue)) {
            return false;
        }
        HexBinaryValue other = (HexBinaryValue) o;
        return Arrays.equals(bytes, other.bytes);
    }
}