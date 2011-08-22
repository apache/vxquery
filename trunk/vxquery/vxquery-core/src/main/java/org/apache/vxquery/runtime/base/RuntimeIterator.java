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
package org.apache.vxquery.runtime.base;

import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.CallStackFrame;

public interface RuntimeIterator extends EagerEvaluator {
    public void open(CallStackFrame frame);

    public Object next(CallStackFrame frame) throws SystemException;

    /**
     * Positions the iterator after skipping said number of items. This call may
     * skip
     * fewer items if it reaches the end of the sequence.
     * 
     * @param frame
     *            - The call stack frame.
     * @param len
     *            - Number of items to skip.
     * @return len - n where n is the number of items actually skipped.
     * @throws SystemException
     */
    public int skip(CallStackFrame frame, int len) throws SystemException;

    public void close(CallStackFrame frame);
}