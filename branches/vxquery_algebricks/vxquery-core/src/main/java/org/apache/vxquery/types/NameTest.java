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

import javax.xml.namespace.QName;

public final class NameTest {
    public static final String WILDCARD = null;

    public static final NameTest STAR_NAMETEST = new NameTest(null, null);

    private String uri;
    private String localName;

    public NameTest(String uri, String localName) {
        this.uri = uri;
        this.localName = localName;
    }

    public String getUri() {
        return uri;
    }

    public String getLocalName() {
        return localName;
    }

    public QName asQName() {
        if (uri == null || localName == null) {
            throw new UnsupportedOperationException();
        }
        return new QName(uri, localName);
    }

    @Override
    public String toString() {
        return "NameTest(" + asQName() + ")";
    }
}