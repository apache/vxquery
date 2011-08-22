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
package org.apache.vxquery.runtime.paths;

import java.util.Arrays;

import org.apache.vxquery.runtime.base.CloseableIterator;

final class PathStack {
    private static final int INITIAL_SIZE = 16;
    private static final int INCREMENT = 32;

    CloseableIterator[] cis;
    int sTop;

    PathStack() {
        cis = new CloseableIterator[INITIAL_SIZE];
        sTop = -1;
    }

    void push(CloseableIterator seq) {
        ensureStackspace();
        cis[++sTop] = seq;
    }

    void pop() {
        CloseableIterator ci = cis[sTop--];
        ci.close();
    }

    CloseableIterator peek() {
        return cis[sTop];
    }

    void reset() {
        sTop = -1;
    }

    boolean isEmpty() {
        return sTop <= -1;
    }

    private void ensureStackspace() {
        if (sTop >= cis.length - 1) {
            cis = Arrays.copyOf(cis, cis.length + INCREMENT);
        }
    }
}