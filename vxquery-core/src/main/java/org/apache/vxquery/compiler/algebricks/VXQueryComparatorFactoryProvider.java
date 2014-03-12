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
package org.apache.vxquery.compiler.algebricks;

import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.values.ValueTag;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class VXQueryComparatorFactoryProvider implements IBinaryComparatorFactoryProvider {
    @Override
    public IBinaryComparatorFactory getBinaryComparatorFactory(Object type, boolean ascending)
            throws AlgebricksException {
        return new BinaryComparatorFactory(type, ascending);
    }

    private static class BinaryComparatorFactory implements IBinaryComparatorFactory {
        private static final long serialVersionUID = 1L;

        private final boolean ascending;

        public BinaryComparatorFactory(Object type, boolean ascending) {
            this.ascending = ascending;
        }

        @Override
        public IBinaryComparator createBinaryComparator() {
            final TaggedValuePointable tvp1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
            final TaggedValuePointable tvp2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
            final SequencePointable sp1 = (SequencePointable) SequencePointable.FACTORY.createPointable();
            final SequencePointable sp2 = (SequencePointable) SequencePointable.FACTORY.createPointable();
            return new IBinaryComparator() {
                @Override
                public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                    tvp1.set(b1, s1, l1);
                    tvp2.set(b2, s2, l2);
                    if (tvp1.getTag() != tvp2.getTag()) {
                        return tvp1.getTag() - tvp2.getTag();
                    }
                    // Empty sequences do not match. Supports hash based join.
                    switch (tvp1.getTag()) {
                        case ValueTag.SEQUENCE_TAG:
                            tvp1.getValue(sp1);
                            if (sp1.getEntryCount() == 0) {
                                return -1;
                            }

                            switch (tvp2.getTag()) {
                                case ValueTag.SEQUENCE_TAG:
                                    tvp2.getValue(sp2);
                                    if (sp2.getEntryCount() == 0) {
                                        return 1;
                                    }
                                    break;
                                default:
                            }

                            break;
                        default:
                    }
                    // Do a binary compare between byte arrays.
                    for (int i = 0; i < b1.length && i < b2.length; i++) {
                        if (b1[i] != b2[i]) {
                            return b1[i] - b2[i];
                        }
                    }
                    return b1.length - b2.length;
                }
            };
        }
    }
}