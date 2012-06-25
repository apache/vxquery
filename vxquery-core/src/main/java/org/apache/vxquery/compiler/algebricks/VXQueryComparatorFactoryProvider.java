package org.apache.vxquery.compiler.algebricks;

import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;

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
            final TaggedValuePointable tvp1 = new TaggedValuePointable();
            final TaggedValuePointable tvp2 = new TaggedValuePointable();
            return new IBinaryComparator() {
                @Override
                public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                    tvp1.set(b1, s1, l1);
                    tvp2.set(b2, s2, l2);
                    return 0;
                }
            };
        }
    }
}