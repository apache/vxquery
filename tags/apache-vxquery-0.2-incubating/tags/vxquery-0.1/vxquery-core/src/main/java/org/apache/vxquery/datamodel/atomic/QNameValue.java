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
package org.apache.vxquery.datamodel.atomic;

import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.types.AtomicType;
import org.apache.vxquery.types.BuiltinTypeRegistry;

public class QNameValue extends AtomicValue {
    protected NameCache nameCache;

    protected int code;

    QNameValue(NameCache nameCache, int code) {
        this(nameCache, code, BuiltinTypeRegistry.XS_QNAME);
    }

    QNameValue(NameCache nameCache, int code, AtomicType type) {
        super(type);
        this.nameCache = nameCache;
        this.code = code;
    }

    @Override
    public CharSequence getStringValue() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(nameCache.getPrefix(code)).append(':').append(nameCache.getLocalName(code));
        return buffer;
    }

    public NameCache getNameCache() {
        return nameCache;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String toString() {
        return "[QNAME_VALUE " + code + "]";
    }

    public String getUri() {
        return nameCache.getUri(code);
    }

    public String getLocalName() {
        return nameCache.getLocalName(code);
    }

    public String getPrefix() {
        return nameCache.getPrefix(code);
    }
}