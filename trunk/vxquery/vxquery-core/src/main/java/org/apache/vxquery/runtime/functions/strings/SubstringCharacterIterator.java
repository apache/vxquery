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
package org.apache.vxquery.runtime.functions.strings;

public class SubstringCharacterIterator implements ICharacterIterator {
    private final ICharacterIterator in;
    private int start;
    private int length;
    private int charOffset;

    public SubstringCharacterIterator(ICharacterIterator in) {
        this.in = in;
    }

    public void setBounds(int start, int length) {
        this.start = start;
        this.length = length;
    }

    @Override
    public char next() {
        char c = in.next();
        // Only drill down if there is more to the string.
        if (c == ICharacterIterator.EOS_CHAR) {
            return c;
        } else if (charOffset < start) {
            ++charOffset;
            return next();
        } else if (charOffset < start + length || length == Integer.MAX_VALUE) {
            ++charOffset;
            return c;
        }
        return (char) 0;
    }

    @Override
    public void reset() {
        in.reset();
        charOffset = 1;
    }

}
