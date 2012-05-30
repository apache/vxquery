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
package org.apache.vxquery.v0runtime.core;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.v0datamodel.DMOKind;
import org.apache.vxquery.v0datamodel.XDMSequence;
import org.apache.vxquery.v0datamodel.XDMValue;
import org.apache.vxquery.v0runtime.base.CloseableIterator;

public final class Deflater implements CloseableIterator {
    private CloseableIterator input;

    private CloseableIterator sequence;

    public void reset(CloseableIterator input) {
        this.input = input;
        sequence = null;
    }

    @Override
    public void close() {
        if (sequence != null) {
            sequence.close();
        }
        input.close();
    }

    @Override
    public Object next() throws SystemException {
        while (true) {
            if (sequence != null) {
                Object o = sequence.next();
                if (o != null) {
                    return o;
                }
                sequence.close();
                sequence = null;
            }
            XDMValue v = (XDMValue) input.next();
            if (v != null && v.getDMOKind() == DMOKind.SEQUENCE) {
                sequence = ((XDMSequence) v).createItemIterator();
                continue;
            }
            return v;
        }
    }
}