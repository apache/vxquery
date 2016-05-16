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
package org.apache.vxquery.datamodel.accessors.atomic;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.IntegerPointable;

public class CodedQNamePointable extends AbstractPointable {
    public static final int SIZE = 12;

    private static final int OFF_PREFIX = 0;
    private static final int OFF_NS = 4;
    private static final int OFF_LOCAL = 8;

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return SIZE;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }

        @Override
        public IPointable createPointable() {
            return new CodedQNamePointable();
        }
    };

    public int getPrefixCode() {
        return IntegerPointable.getInteger(bytes, start + OFF_PREFIX);
    }

    public int getNamespaceCode() {
        return IntegerPointable.getInteger(bytes, start + OFF_NS);
    }

    public int getLocalCode() {
        return IntegerPointable.getInteger(bytes, start + OFF_LOCAL);
    }
}
