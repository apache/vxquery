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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

public class UTF8StringCharacterIterator implements ICharacterIterator {
    private static final Logger LOGGER = Logger.getLogger(UTF8StringCharacterIterator.class.getName());

    private int byteOffset;
    private final UTF8StringPointable stringp;

    public UTF8StringCharacterIterator(UTF8StringPointable stringp) {
        this.stringp = stringp;
    }

    public int getByteOffset() {
        return byteOffset;
    }

    @Override
    public char next() {
        // Default - no character exists.
        int c = ICharacterIterator.EOS_CHAR;
        if (byteOffset < stringp.getLength()) {
            c = stringp.charAt(byteOffset);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.finer("  UTF8StringCharacterIterator char[" + byteOffset + "] = " + c);
            }
            // Increment cursor
            byteOffset += stringp.charSize(byteOffset);
        }
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.finer("  END UTF8StringCharacterIterator char[" + byteOffset + "] = " + c);
        }
        return (char) c;
    }

    @Override
    public void reset() {
        byteOffset = stringp.getMetaDataLength();
    }

    public void setByteOffset(int byteOffset) {
        this.byteOffset = byteOffset;
    }

}
