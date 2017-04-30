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

import java.io.Serializable;
import java.util.Arrays;

import org.apache.hyracks.util.string.UTF8StringUtil;

public final class NameTest implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String WILDCARD = null;

    public static final NameTest STAR_NAMETEST = new NameTest(null, null);

    private byte[] uri;
    private byte[] localName;

    public NameTest(byte[] uri, byte[] localName) {
        this.uri = uri;
        this.localName = localName;
    }

    public byte[] getUri() {
        return uri;
    }

    public byte[] getLocalName() {
        return localName;
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("NameTest({");
        if (uri != null) {
            UTF8StringUtil.toString(buffer, uri, 0);
        } else {
            buffer.append('*');
        }
        buffer.append('}');
        if (localName != null) {
            UTF8StringUtil.toString(buffer, localName, 0);
        } else {
            buffer.append('*');
        }
        return buffer.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((localName == null) ? 0 : Arrays.hashCode(localName));
        result = prime * result + ((uri == null) ? 0 : Arrays.hashCode(uri));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NameTest other = (NameTest) obj;
        if (localName == null) {
            if (other.localName != null)
                return false;
        } else if (!Arrays.equals(localName, other.localName))
            return false;
        if (uri == null) {
            if (other.uri != null)
                return false;
        } else if (!Arrays.equals(uri, other.uri))
            return false;
        return true;
    }
}
