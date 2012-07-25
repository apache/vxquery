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
package org.apache.vxquery.datamodel;

import org.apache.vxquery.collations.CodepointCollation;
import org.apache.vxquery.collations.Collation;
import org.apache.vxquery.datamodel.atomic.compare.ComparisonUtils;
import org.apache.vxquery.datamodel.atomic.compare.ValueEqComparator;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.RuntimeControlBlock;
import org.apache.vxquery.runtime.base.CloseableIterator;

public final class NodeEqComparator {
    public static final NodeEqComparator INSTANCE = new NodeEqComparator();

    private Collation collation;

    private NodeEqComparator() {
        this(CodepointCollation.INSTANCE);
    }

    private NodeEqComparator(Collation collation) {
        this.collation = collation;
    }

    public static NodeEqComparator get(Collation collation) {
        if (collation == null || collation == CodepointCollation.INSTANCE) {
            return INSTANCE;
        } else {
            return new NodeEqComparator(collation);
        }
    }

    public boolean compareDocument(XDMNode n1, XDMNode n2) {
        throw new UnsupportedOperationException(); // TODO implement compareDocument
    }

    public boolean compareElement(XDMNode n1, XDMNode n2, RuntimeControlBlock rcb) throws SystemException {
        if (!compareNodeName(n1, n2)) {
            return false;
        }
        // TODO check and compare types - right now everything is untyped
        if (n1.hasAttributes()) {
            if (!n2.hasAttributes() || !compareAttributes(n1, n2, rcb)) {
                return false;
            }
        } else if (n2.hasAttributes()) {
            return false;
        }

        CloseableIterator ci1 = n1.getChildren();
        CloseableIterator ci2 = n2.getChildren();
        Object child1;
        while ((child1 = ci1.next()) != null) {
            // TODO need to skip everything that's not an element or a text node
            Object child2 = ci2.next();
            if (child2 == null) {
                ci1.close();
                ci2.close();
                return false;
            }
            XDMNode chn1 = (XDMNode) child1;
            XDMNode chn2 = (XDMNode) child2;
            if (chn1.getDMOKind() != chn2.getDMOKind()) {
                ci1.close();
                ci2.close();
                return false;
            }
            switch (chn1.getDMOKind()) {
                case ELEMENT_NODE:
                    if (!compareElement(chn1, chn2, rcb)) {
                        ci1.close();
                        ci2.close();
                        return false;
                    }
                    break;
                case TEXT_NODE:
                    if (!compareText(chn1, chn2)) {
                        ci1.close();
                        ci2.close();
                        return false;
                    }
                    break;
                case COMMENT_NODE:
                    break;
                default:
                    throw new IllegalStateException(chn1.getDMOKind().toString());
            }
        }
        ci1.close();
        boolean result = ci2.next() == null;
        ci2.close();

        return result;
    }

    private boolean compareAttributes(XDMNode n1, XDMNode n2, RuntimeControlBlock rcb) throws SystemException {
        CloseableIterator ci1 = n1.getAttributes();
        CloseableIterator ci2 = null;
        Object o1 = ci1.next();
        int c1 = 0;
        int c2 = 0;
        while (o1 != null) {
            ++c1;
            if (ci2 != null) {
                ci2.close();
            }
            c2 = 0;
            ci2 = n2.getAttributes();
            Object o2 = ci2.next();
            while (o2 != null) {
                ++c2;
                if (compareAttribute((XDMNode) o1, (XDMNode) o2, rcb)) {
                    break;
                }
                o2 = ci2.next();
            }
            if (o2 == null) {
                ci1.close();
                ci2.close();
                return false;
            }
            o1 = ci1.next();
        }
        ci1.close();
        if (ci2 != null) {
            while (ci2.next() != null) {
                if (++c2 > c1) {
                    ci2.close();
                    return false;
                }
            }
        }
        ci2.close();
        return true;
    }

    public boolean compareAttribute(XDMNode n1, XDMNode n2, RuntimeControlBlock rcb) throws SystemException {
        if (!compareNodeName(n1, n2)) {
            return false;
        }
        XDMValue v1 = n1.getTypedValue();
        XDMValue v2 = n2.getTypedValue();
        if (v1.getDMOKind() != v2.getDMOKind()) {
            return false;
        } else if (v1.getDMOKind() == DMOKind.SEQUENCE) {
            CloseableIterator ci1 = ((XDMSequence) v1).createItemIterator();
            CloseableIterator ci2 = ((XDMSequence) v2).createItemIterator();
            Object o1 = ci1.next();
            while (o1 != null) {
                Object o2 = ci2.next();
                if (o2 == null || !compareAtomic((XDMAtomicValue) o1, (XDMAtomicValue) o2, rcb)) {
                    ci1.close();
                    ci2.close();
                    return false;
                }
                o1 = ci1.next();
            }
            final boolean result = ci2.next() == null ? true : false;
            ci1.close();
            ci2.close();
            return result;
        }
        return compareAtomic((XDMAtomicValue) v1, (XDMAtomicValue) v2, rcb);
    }

    public boolean compareNamespace(XDMNode n1, XDMNode n2) {
        throw new UnsupportedOperationException(); // TODO implement compareNamespace
    }

    public boolean compareProcessingInstruction(XDMNode n1, XDMNode n2) {
        assert n1.getDMOKind() == DMOKind.PI_NODE;
        assert n2.getDMOKind() == DMOKind.PI_NODE;
        return compareNodeName(n1, n2) && compareStringValue(n1, n2);
    }

    public boolean compareComment(XDMNode n1, XDMNode n2) {
        assert n1.getDMOKind() == DMOKind.COMMENT_NODE;
        assert n2.getDMOKind() == DMOKind.COMMENT_NODE;
        return compareStringValue(n1, n2);
    }

    public boolean compareText(XDMNode n1, XDMNode n2) {
        assert n1.getDMOKind() == DMOKind.TEXT_NODE;
        assert n2.getDMOKind() == DMOKind.TEXT_NODE;
        return compareStringValue(n1, n2);
    }

    private boolean compareStringValue(XDMNode n1, XDMNode n2) {
        return collation.getComparator().compare(n1.getStringValue(), n2.getStringValue()) == 0;
    }

    private boolean compareNodeName(XDMNode n1, XDMNode n2) {
        try {
            return ValueEqComparator.INSTANCE.compareQName(n1.getNodeName(), n2.getNodeName());
        } catch (SystemException e) {
            // this should be impossible
            throw new IllegalStateException(e);
        }
    }

    public boolean compareAtomic(XDMAtomicValue a1, XDMAtomicValue a2, RuntimeControlBlock rcb) throws SystemException {
        Boolean result = ComparisonUtils.valueCompare(rcb, a1, a2, ValueEqComparator.INSTANCE, collation);
        if (result == null) {
            return false;
        } else {
            // TODO if both are NaN we need to return true as well
            return result;
        }
    }
}
