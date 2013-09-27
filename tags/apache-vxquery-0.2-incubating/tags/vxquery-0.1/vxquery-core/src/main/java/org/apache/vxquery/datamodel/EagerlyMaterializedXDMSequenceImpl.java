/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.datamodel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.base.CloseableIterator;
import org.apache.vxquery.runtime.base.CloseableSkippableIterator;

public final class EagerlyMaterializedXDMSequenceImpl implements XDMSequence {
    private List<XDMItem> list;

    public EagerlyMaterializedXDMSequenceImpl(List<XDMItem> list) {
        this.list = list;
    }

    public static XDMValue inflate(CloseableIterator iterator) throws SystemException {
        try {
            XDMValue v1 = (XDMValue) iterator.next();
            if (v1 == null) {
                return null;
            }
            XDMValue v2 = (XDMValue) iterator.next();
            if (v2 == null) {
                return v1;
            }
            List<XDMItem> seqList = new ArrayList<XDMItem>();
            addValueToSequence(v1, seqList);
            addValueToSequence(v2, seqList);
            XDMValue v;
            while ((v = (XDMValue) iterator.next()) != null) {
                addValueToSequence(v, seqList);
            }
            return new EagerlyMaterializedXDMSequenceImpl(seqList);
        } finally {
            iterator.close();
        }
    }

    private static void addValueToSequence(XDMValue v, List<XDMItem> list) throws SystemException {
        if (v.getDMOKind() == DMOKind.SEQUENCE) {
            ((XDMSequence) v).appendToList(list);
        } else {
            list.add((XDMItem) v);
        }
    }

    @Override
    public CloseableSkippableIterator createItemIterator() {
        return new CloseableSkippableIterator() {
            private int i = 0;
            private int length = list.size();

            @Override
            public void close() {
            }

            @Override
            public Object next() throws SystemException {
                if (i < length) {
                    return list.get(i++);
                }
                return null;
            }

            @Override
            public int skip(int len) {
                if (i < length) {
                    i += len;
                    return i > len ? i - length : 0;
                }
                return len;
            }
        };
    }

    @Override
    public DMOKind getDMOKind() {
        return DMOKind.SEQUENCE;
    }

    @Override
    public void appendToList(List<XDMItem> list) {
        list.addAll(this.list);
    }

    @Override
    public List<XDMItem> getAsImmutableList() {
        return Collections.unmodifiableList(list);
    }
}