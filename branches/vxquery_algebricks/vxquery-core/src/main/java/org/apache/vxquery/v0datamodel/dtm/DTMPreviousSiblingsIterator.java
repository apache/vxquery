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

final class DTMPreviousSiblingsIterator implements CloseableIterator {
    private DTMNodeImpl focus;
    private int index;

    public DTMPreviousSiblingsIterator(DTMNodeImpl focus) {
        this.focus = focus;
        DTM dtm = focus.getDTM();
        int fIndex = focus.getDTMIndex();
        dtm.ensurePreviousIndexPresent();
        index = dtm.previous[fIndex];
    }

    @Override
    public void close() {
        index = DTM.NULL;
    }

    @Override
    public Object next() throws SystemException {
        if (index == DTM.NULL) {
            return null;
        }
        DTM dtm = focus.getDTM();
        DTMNodeImpl next = new DTMNodeImpl(dtm, index);
        index = dtm.previous[index];
        return next;
    }
}