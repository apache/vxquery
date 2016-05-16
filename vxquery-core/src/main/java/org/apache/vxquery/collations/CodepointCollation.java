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
package org.apache.vxquery.collations;

import java.util.Comparator;

public class CodepointCollation implements Collation {
    public static final String URI = "http://www.w3.org/2005/xpath-functions/collation/codepoint";

    private static final Comparator<CharSequence> COMPARATOR = new Comparator<CharSequence>() {
        @Override
        public int compare(CharSequence o1, CharSequence o2) {
            int l1 = o1.length();
            int l2 = o2.length();
            int i1 = 0;
            int i2 = 0;
            while (true) {
                if (i1 >= l1) {
                    return i2 >= 0 ? 0 : -1;
                }
                if (i2 >= l2) {
                    return 1;
                }
                int cp1 = Character.codePointAt(o1, i1);
                int cp2 = Character.codePointAt(o2, i2);
                if (cp1 < cp2) {
                    return -1;
                } else if (cp1 > cp2) {
                    return 1;
                }
                i1 += Character.charCount(cp1);
                i2 += Character.charCount(cp2);
            }
        }
    };

    public static final Collation INSTANCE = new CodepointCollation();

    private CodepointCollation() {
    }

    @Override
    public boolean contains(CharSequence cs1, CharSequence cs2) {
        return cs1.toString().contains(cs2);
    }

    @Override
    public boolean endsWith(CharSequence cs1, CharSequence cs2) {
        return cs1.toString().endsWith(cs2.toString());
    }

    @Override
    public Comparator<CharSequence> getComparator() {
        return COMPARATOR;
    }

    @Override
    public boolean startsWith(CharSequence cs1, CharSequence cs2) {
        return cs1.toString().startsWith(cs2.toString());
    }

    @Override
    public CharSequence substringAfter(CharSequence cs1, CharSequence cs2) {
        String s1 = cs1.toString();
        String s2 = cs2.toString();
        int idx = s1.indexOf(s2);
        return idx < 0 ? "" : s1.substring(idx + s2.length());
    }

    @Override
    public CharSequence substringBefore(CharSequence cs1, CharSequence cs2) {
        String s1 = cs1.toString();
        String s2 = cs2.toString();
        int idx = s1.indexOf(s2);
        return idx < 0 ? "" : s1.substring(0, idx);
    }

    @Override
    public boolean supportsStringMatching() {
        return true;
    }
}
