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
import org.apache.vxquery.v0runtime.CallStackFrame;
import org.apache.vxquery.v0runtime.base.CloseableIterator;
import org.apache.vxquery.v0runtime.base.RuntimeIterator;

public final class CloseableIteratorAdapter implements CloseableIterator {
    private final CallStackFrame frame;
    private final RuntimeIterator ri;

    public CloseableIteratorAdapter(CallStackFrame frame, RuntimeIterator ri) {
        this.frame = frame;
        this.ri = ri;
    }

    @Override
    public void close() {
        ri.close(frame);
    }

    @Override
    public Object next() throws SystemException {
        return ri.next(frame);
    }
}