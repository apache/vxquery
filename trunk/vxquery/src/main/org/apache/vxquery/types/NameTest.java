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

import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.util.Filter;

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

    @SuppressWarnings("unchecked")
    public Filter<XDMNode> createNameMatchFilter(final NameCache nameCache) {
        if (uri == null) {
            if (localName == null) {
                return (Filter<XDMNode>) Filter.TRUE_FILTER;
            } else {
                return new Filter<XDMNode>() {
                    @Override
                    public boolean accept(XDMNode value) throws SystemException {
                        return nameCache.getLocalName(value.getNodeNameCode()).equals(localName);
                    }
                };
            }
        } else {
            if (localName == null) {
                return new Filter<XDMNode>() {
                    @Override
                    public boolean accept(XDMNode value) throws SystemException {
                        return nameCache.probeUriCode(uri) == nameCache.getUriCode(value.getNodeNameCode());
                    }
                };
            } else {
                final int uriCode = NameCache.removePrefix(nameCache.intern("", uri, localName));
                return new Filter<XDMNode>() {
                    @Override
                    public boolean accept(XDMNode value) throws SystemException {
                        return NameCache.removePrefix(value.getNodeNameCode()) == uriCode;
                    }
                };
            }
        }
    }
}