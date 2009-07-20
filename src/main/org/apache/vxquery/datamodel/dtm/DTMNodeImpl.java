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
package org.apache.vxquery.datamodel.dtm;

import java.nio.CharBuffer;

import org.apache.vxquery.datamodel.DMOKind;
import org.apache.vxquery.datamodel.NameCache;
import org.apache.vxquery.datamodel.XDMNode;
import org.apache.vxquery.datamodel.XDMValue;
import org.apache.vxquery.datamodel.atomic.AnyUriValue;
import org.apache.vxquery.runtime.base.CloseableIterator;
import org.apache.vxquery.runtime.base.OpenableCloseableIterator;
import org.apache.vxquery.runtime.core.SingletonCloseableIterator;

final class DTMNodeImpl implements XDMNode {
    private DTM dtm;
    private int index;

    DTMNodeImpl(DTM dtm, int index) {
        this.dtm = dtm;
        this.index = index;
    }

    @Override
    public Object getImplementationIdentifier() {
        return DTM.class;
    }

    @Override
    public int compareDocumentOrder(XDMNode other) {
        Object iid = getImplementationIdentifier();
        Object oiid = other.getImplementationIdentifier();
        if (iid.equals(oiid)) {
            DTMNodeImpl o = (DTMNodeImpl) other;
            if (dtm == o.dtm) {
                return index == o.index ? 0 : (index < o.index ? -1 : 1);
            }
        }
        int hc = iid.hashCode();
        int ohc = oiid.hashCode();
        return hc == ohc ? 0 : (hc < ohc ? -1 : 1);
    }

    @Override
    public XDMValue getAtomizedValue() {
        switch (dtm.nodeKind[index]) {
            case DTM.DTM_ATTRIBUTE:
            case DTM.DTM_ELEMENT:
            case DTM.DTM_DOCUMENT:
            case DTM.DTM_TEXT:
                return dtm.getAtomicValueFactory().createUntypedAtomic(getStringValue());
        }
        return dtm.getAtomicValueFactory().createString(getStringValue());
    }

    @Override
    public AnyUriValue getBaseUri() {
        return null;
    }

    @Override
    public AnyUriValue getDocumentUri() {
        return null;
    }

    @Override
    public boolean getIsNilled() {
        return false;
    }

    @Override
    public NameCache getNameCache() {
        return dtm.nameCache;
    }

    @Override
    public int getNodeNameCode() {
        return dtm.nameCode[index];
    }

    @Override
    public XDMNode getParent() {
        return null;
    }

    @Override
    public int getTypeNameCode() {
        return 0;
    }

    @Override
    public OpenableCloseableIterator getTypedValue() {
        return new SingletonCloseableIterator(getAtomizedValue());
    }

    @Override
    public boolean isID() {
        return false;
    }

    @Override
    public boolean isIDREFS() {
        return false;
    }

    @Override
    public boolean isSameNode(XDMNode other) {
        if (!(other instanceof DTMNodeImpl)) {
            return false;
        }
        DTMNodeImpl o = (DTMNodeImpl) other;
        return o.dtm == dtm && o.index == index;
    }

    @Override
    public CharSequence getStringValue() {
        switch (dtm.nodeKind[index]) {
            case DTM.DTM_ATTRIBUTE:
            case DTM.DTM_COMMENT:
            case DTM.DTM_PI:
                return CharBuffer.wrap(dtm.auxContent, dtm.param0[index], dtm.param1[index]);

            case DTM.DTM_DOCUMENT:
                return CharBuffer.wrap(dtm.textContent);

            case DTM.DTM_ELEMENT:
                int ci = index;
                int ubound;
                while (true) {
                    ubound = dtm.next[ci];
                    if (ubound == DTM.NULL) {
                        ubound = dtm.nNodes;
                        break;
                    } else if (ubound < ci) {
                        ci = ubound;
                    } else {
                        break;
                    }
                }
                int start = DTM.NULL;
                int end = DTM.NULL;
                for (int i = index + 1; i < ubound; ++i) {
                    if (dtm.nodeKind[i] == DTM.DTM_TEXT) {
                        if (start == DTM.NULL) {
                            start = i;
                        }
                        end = i;
                    }
                }
                if (start == DTM.NULL) {
                    return "";
                }
                return CharBuffer.wrap(dtm.textContent, dtm.param0[start], dtm.param0[end] + dtm.param1[end]
                        - dtm.param0[start]);

            case DTM.DTM_TEXT:
                return CharBuffer.wrap(dtm.textContent, dtm.param0[index], dtm.param1[index]);
        }
        throw new IllegalStateException("Unknown node kind: " + dtm.nodeKind[index]);
    }

    @Override
    public DMOKind getDMOKind() {
        switch (dtm.nodeKind[index]) {
            case DTM.DTM_ATTRIBUTE:
                return DMOKind.ATTRIBUTE_NODE;

            case DTM.DTM_COMMENT:
                return DMOKind.COMMENT_NODE;

            case DTM.DTM_DOCUMENT:
                return DMOKind.DOCUMENT_NODE;

            case DTM.DTM_ELEMENT:
                return DMOKind.ELEMENT_NODE;

            case DTM.DTM_PI:
                return DMOKind.PI_NODE;

            case DTM.DTM_TEXT:
                return DMOKind.TEXT_NODE;
        }
        throw new IllegalStateException("Unknown node kind: " + dtm.nodeKind[index]);
    }

    @Override
    public boolean hasAttributes() {
        if (dtm.nodeKind[index] == DTM.DTM_ELEMENT) {
            int i = index + 1;
            while (i < dtm.nNodes && dtm.nodeKind[i] == DTM.DTM_NAMESPACE) {
                ++i;
            }
            return i < dtm.nNodes && dtm.nodeKind[i] == DTM.DTM_ATTRIBUTE;
        }
        return false;
    }

    @Override
    public boolean hasChildren() {
        byte kind = dtm.nodeKind[index];
        if (kind == DTM.DTM_DOCUMENT || kind == DTM.DTM_ELEMENT) {
            return dtm.param1[index] != DTM.NULL;
        }
        return false;
    }

    @Override
    public CloseableIterator getAttributes() {
        return new DTMAttributeIterator(this);
    }

    @Override
    public CloseableIterator getChildren() {
        return new DTMChildrenIterator(this);
    }

    @Override
    public CloseableIterator getFollowingSiblings() {
        return new DTMFollowingSiblingsIterator(this);
    }

    @Override
    public CloseableIterator getPrecedingSiblings() {
        return new DTMPreviousSiblingsIterator(this);
    }

    DTM getDTM() {
        return dtm;
    }

    int getDTMIndex() {
        return index;
    }
}