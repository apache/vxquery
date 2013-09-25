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
package org.apache.vxquery.datamodel;

import java.util.Arrays;

public class StringCache {
    private static final int INITIAL_SIZE = 128;

    private String[] strings;
    private int count;

    public StringCache() {
        strings = new String[INITIAL_SIZE];
        count = 0;
    }

    public StringCache(StringCache cache) {
        strings = Arrays.copyOf(cache.strings, cache.strings.length);
        count = cache.count;
    }

    public int intern(String str, boolean insert) {
        for (int i = 0; i < count; ++i) {
            if (strings[i].equals(str)) {
                return i;
            }
        }
        if (!insert) {
            return -1;
        }
        if (count >= strings.length) {
            String[] temp = new String[strings.length * 2];
            System.arraycopy(strings, 0, temp, 0, strings.length);
            strings = temp;
        }
        strings[count] = str;
        return count++;
    }

    public String get(int code) {
        return strings[code];
    }
}