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

public final class NameCache {
    private static final int INITIAL_PREFIX_ARRAY_LENGTH = 100;

    private static final int INITIAL_URI_ARRAY_LENGTH = 100;

    private static final int HASHTABLE_LENGTH = 1024;

    private static final int INITIAL_HASHTABLE_ENTRYLIST_LENGTH = 8;

    private String[] prefixes;
    private int prefixCount;

    private String[] uris;
    private int uriCount;

    private NameEntry[][] hashTable;

    public NameCache() {
        prefixes = new String[INITIAL_PREFIX_ARRAY_LENGTH];
        uris = new String[INITIAL_URI_ARRAY_LENGTH];
        hashTable = new NameEntry[HASHTABLE_LENGTH][];
        for (int i = 0; i < HASHTABLE_LENGTH; ++i) {
            hashTable[i] = new NameEntry[INITIAL_HASHTABLE_ENTRYLIST_LENGTH];
        }
        prefixCount = 0;
        uriCount = 0;
    }

    public NameCache(NameCache pool) {
        prefixes = Arrays.copyOf(pool.prefixes, pool.prefixes.length);
        uris = Arrays.copyOf(pool.uris, pool.uris.length);
        hashTable = new NameEntry[pool.hashTable.length][];
        for (int i = 0; i < hashTable.length; ++i) {
            hashTable[i] = Arrays.copyOf(pool.hashTable[i], pool.hashTable[i].length);
        }
        prefixCount = pool.prefixCount;
        uriCount = pool.uriCount;
    }

    private static class NameEntry {
        private final String localName;
        private final int uriCode;

        NameEntry(String localName, int uriCode, int prefixCode) {
            this.localName = localName;
            this.uriCode = uriCode;
        }
    }

    public int intern(String prefix, String uri, String localName) {
        int prefixCode = internPrefix(prefix, true);
        int uriCode = internUri(uri, true);
        int hash = Math.abs(hash(localName)) % HASHTABLE_LENGTH;

        int idx = 0;
        NameEntry[] entryList = hashTable[hash];
        while (idx < entryList.length) {
            NameEntry entry = entryList[idx];
            if (entry == null || (entry.uriCode == uriCode && entry.localName.equals(localName))) {
                break;
            }
            ++idx;
        }
        if (idx >= entryList.length || entryList[idx] == null) {
            NameEntry entry = new NameEntry(localName, uriCode, prefixCode);

            if (idx >= entryList.length) {
                NameEntry[] temp = new NameEntry[entryList.length * 2];
                System.arraycopy(entryList, 0, temp, 0, entryList.length);
            }
            entryList[idx] = entry;
        }
        return (prefixCode << 24) + (idx << 10) + hash;
    }

    private int hash(String cs) {
        return cs.hashCode();
    }

    public int probe(String prefix, String uri, String localName) {
        int prefixCode = internPrefix(prefix, false);
        if (prefixCode == -1) {
            return -1;
        }
        int uriCode = internUri(uri, false);
        if (uriCode == -1) {
            return -1;
        }
        int hash = Math.abs(hash(localName)) % HASHTABLE_LENGTH;

        int idx = 0;
        NameEntry[] entryList = hashTable[hash];
        while (idx < entryList.length) {
            NameEntry entry = entryList[idx];
            if (entry == null || (entry.uriCode == uriCode && entry.localName.equals(localName))) {
                break;
            }
            ++idx;
        }
        if (idx >= entryList.length || entryList[idx] == null) {
            return -1;
        }
        return (prefixCode << 24) + (idx << 10) + hash;
    }

    private int internPrefix(String prefix, boolean insert) {
        for (int i = 0; i < prefixCount; ++i) {
            if (prefixes[i].equals(prefix)) {
                return i;
            }
        }
        if (!insert) {
            return -1;
        }
        if (prefixCount >= prefixes.length) {
            String[] temp = new String[prefixes.length * 2];
            System.arraycopy(prefixes, 0, temp, 0, prefixes.length);
            prefixes = temp;
        }
        prefixes[prefixCount] = prefix;
        return prefixCount++;
    }

    private int internUri(String uri, boolean insert) {
        for (int i = 0; i < uriCount; ++i) {
            if (uris[i].equals(uri)) {
                return i;
            }
        }
        if (!insert) {
            return -1;
        }
        if (uriCount >= uris.length) {
            String[] temp = new String[uris.length * 2];
            System.arraycopy(uris, 0, temp, 0, uris.length);
            uris = temp;
        }
        uris[uriCount] = uri;
        return uriCount++;
    }

    public String getPrefix(int code) {
        int prefixCode = code >> 24;
        if (prefixCode == -1) {
            return "";
        }
        return prefixes[prefixCode];
    }

    public String getUri(int code) {
        int hash = code & 0x000003ff;
        int idx = (code >> 10) & 0x00003fff;
        NameEntry entry = hashTable[hash][idx];
        return uris[entry.uriCode];
    }

    public String getLocalName(int code) {
        int hash = code & 0x000003ff;
        int idx = (code >> 10) & 0x00003fff;
        NameEntry entry = hashTable[hash][idx];
        return entry.localName;
    }

    public static int removePrefix(int nameCode) {
        return nameCode & 0xffffff;
    }

    public int getUriCode(int nameCode) {
        int hash = nameCode & 0x000003ff;
        int idx = (nameCode >> 10) & 0x00003fff;
        return hashTable[hash][idx].uriCode;
    }

    public int probeUriCode(String uri) {
        return internUri(uri, false);
    }

    public int translateCode(NameCache otherPool, int code) {
        return otherPool == this ? code : intern(otherPool.getPrefix(code), otherPool.getUri(code), otherPool
                .getLocalName(code));
    }

    public static boolean hasPrefix(int code) {
        return (code & 0xff000000) != 0;
    }
}