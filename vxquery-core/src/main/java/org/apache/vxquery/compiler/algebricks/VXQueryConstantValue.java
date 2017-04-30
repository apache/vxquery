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
package org.apache.vxquery.compiler.algebricks;

import org.apache.commons.codec.binary.Hex;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.vxquery.types.SequenceType;

public class VXQueryConstantValue implements IAlgebricksConstantValue {
    private final SequenceType type;

    private final byte[] value;

    public VXQueryConstantValue(SequenceType type, byte[] value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public boolean isFalse() {
        return false;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public boolean isTrue() {
        return false;
    }

    public SequenceType getType() {
        return type;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(type).append(" (bytes[").append(value.length).append("] = [").append(Hex.encodeHexString(value))
                .append("])");
        return buffer.toString();
    }

    @Override
    public boolean isMissing() {
        return false;
    }
}
