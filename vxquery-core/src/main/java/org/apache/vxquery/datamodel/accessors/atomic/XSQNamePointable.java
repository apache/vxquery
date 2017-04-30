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
package org.apache.vxquery.datamodel.accessors.atomic;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * The XSQNamePointable holds three strings: URI, Prefix and Local Name.
 */
public class XSQNamePointable extends AbstractPointable {
    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return false;
        }

        @Override
        public int getFixedLength() {
            return 0;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new XSQNamePointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public int getUriLength() {
        return getUriLength(bytes, start);
    }

    public static int getUriLength(byte[] bytes, int start) {
        int utfLength = getUriUTFLength(bytes, start);
        return utfLength + UTF8StringUtil.getNumBytesToStoreLength(utfLength);
    }

    public int getUriUTFLength() {
        return getUriUTFLength(bytes, start);
    }

    public static int getUriUTFLength(byte[] bytes, int start) {
        return UTF8StringUtil.getUTFLength(bytes, start);
    }

    public int getPrefixLength() {
        return getPrefixLength(bytes, start);
    }

    public static int getPrefixLength(byte[] bytes, int start) {
        int utfLength = getPrefixUTFLength(bytes, start);
        return utfLength + UTF8StringUtil.getNumBytesToStoreLength(utfLength);
    }

    public int getPrefixUTFLength() {
        return getPrefixUTFLength(bytes, start);
    }

    public static int getPrefixUTFLength(byte[] bytes, int start) {
        return UTF8StringUtil.getUTFLength(bytes, start + getUriLength(bytes, start));
    }

    public int getLocalNameLength() {
        return getLocalNameLength(bytes, start);
    }

    public static int getLocalNameLength(byte[] bytes, int start) {
        int utfLength = getLocalNameUTFLength(bytes, start);
        return utfLength + UTF8StringUtil.getNumBytesToStoreLength(utfLength);
    }

    public int getLocalNameUTFLength() {
        return getLocalNameUTFLength(bytes, start);
    }

    public static int getLocalNameUTFLength(byte[] bytes, int start) {
        return UTF8StringUtil.getUTFLength(bytes,
                start + getUriLength(bytes, start) + getPrefixLength(bytes, start));
    }

    public void getUri(IPointable stringp) {
        getUri(bytes, start, stringp);
    }

    public static void getUri(byte[] bytes, int start, IPointable stringp) {
        stringp.set(bytes, start, getUriLength(bytes, start));
    }

    public void getPrefix(IPointable stringp) {
        getPrefix(bytes, start, stringp);
    }

    public static void getPrefix(byte[] bytes, int start, IPointable stringp) {
        stringp.set(bytes, start + getUriLength(bytes, start), getPrefixLength(bytes, start));
    }

    public void getLocalName(IPointable stringp) {
        getLocalName(bytes, start, stringp);
    }

    public static void getLocalName(byte[] bytes, int start, IPointable stringp) {
        stringp.set(bytes, start + getUriLength(bytes, start) + getPrefixLength(bytes, start),
                getLocalNameLength(bytes, start));
    }
}
