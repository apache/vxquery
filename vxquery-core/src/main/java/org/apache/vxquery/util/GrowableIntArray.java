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
package org.apache.vxquery.util;

import java.util.Arrays;

public class GrowableIntArray {
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private static final int DEFAULT_GROWTH = 16;

    private int[] array;

    private int size;

    public GrowableIntArray() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public GrowableIntArray(int initialCapacity) {
        array = new int[initialCapacity];
        size = 0;
    }

    public void clear() {
        size = 0;
    }

    public int[] getArray() {
        return array;
    }

    public int getSize() {
        return size;
    }

    public void append(int value) {
        if (array.length <= size) {
            grow(DEFAULT_GROWTH);
        }
        array[size++] = value;
    }

    public void append(int[] inArray, int start, int length) {
        if (array.length <= size + length) {
            int increment = (((size + length - array.length) - 1) / DEFAULT_GROWTH + 1) * DEFAULT_GROWTH;
            grow(increment);
        }
        System.arraycopy(inArray, start, array, size, length);
        size += length;
    }

    public void insert(int index, int value) {
        if (index >= size) {
            if (index >= array.length) {
                int increment = (((index + 1 - array.length) - 1) / DEFAULT_GROWTH + 1) * DEFAULT_GROWTH;
                grow(increment);
            }
            size = index + 1;
        } else {
            if (size >= array.length) {
                grow(DEFAULT_GROWTH);
            }
            System.arraycopy(array, index, array, index + 1, size - index);
            ++size;
        }
        array[index] = value;
    }

    public void grow(int increment) {
        if (increment < 0) {
            throw new IllegalArgumentException(increment + " < 0");
        }
        if (increment == 0) {
            return;
        }
        array = Arrays.copyOf(array, array.length + increment);
    }
}