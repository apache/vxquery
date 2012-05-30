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
package org.apache.vxquery.v0datamodel.dtm;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0runtime.base.CloseableIterator;

final class DTMChildrenIterator implements CloseableIterator {
    private DTMNodeImpl parent;
    private int index;

    public DTMChildrenIterator(DTMNodeImpl parent) {
        this.parent = parent;
        DTM dtm = parent.getDTM();
        int pIndex = parent.getDTMIndex();
        index = -1;
        if (dtm.nodeKind[pIndex] == DTM.DTM_DOCUMENT || dtm.nodeKind[pIndex] == DTM.DTM_ELEMENT) {
            index = dtm.param1[pIndex];
        }
    }

    @Override
    public void close() {
        index = DTM.NULL;
    }

    @Override
    public Object next() throws SystemException {
        if (index == DTM.NULL || index == parent.getDTMIndex()) {
            return null;
        }
        DTM dtm = parent.getDTM();
        DTMNodeImpl child = new DTMNodeImpl(dtm, index);
        index = dtm.next[index];
        return child;
    }
}