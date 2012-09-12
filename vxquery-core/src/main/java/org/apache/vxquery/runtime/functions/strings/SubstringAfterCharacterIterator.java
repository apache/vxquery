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

public class SubstringAfterCharacterIterator implements ICharacterIterator {
    private ICharacterIterator searchIterator;
    private final UTF8StringCharacterIterator stringIterator;
    private boolean found = false;

    public SubstringAfterCharacterIterator(UTF8StringCharacterIterator stringIterator) {
        this.stringIterator = stringIterator;
    }

    public void setSearch(ICharacterIterator searchIterator) {
        this.searchIterator = searchIterator;
    }

    @Override
    public char next() {
        int c1 = ICharacterIterator.EOS_CHAR;
        if (!found) {
            searchIterator.reset();
            int currentByteOffset = 0;
            boolean firstPass = true;
            while (true) {
                c1 = stringIterator.next();
                int c2 = searchIterator.next();
                if (firstPass) {
                    // Save character and location for next call.
                    currentByteOffset = stringIterator.getByteOffset();
                    firstPass = false;
                }
                if (c2 == ICharacterIterator.EOS_CHAR) {
                    // End of search string. Found the substring.
                    found = true;
                    break;
                }
                if (c1 == ICharacterIterator.EOS_CHAR) {
                    // End of string. Stop.
                    return (char) 0;
                }
                if (c1 != c2) {
                    // No match found. Move the next character.
                    searchIterator.reset();
                    stringIterator.setByteOffset(currentByteOffset);
                    firstPass = true;
                }
            }
        }

        if (c1 != ICharacterIterator.EOS_CHAR) {
            return (char) c1;
        }
        return stringIterator.next();
    }

    @Override
    public void reset() {
        stringIterator.reset();
    }
}
