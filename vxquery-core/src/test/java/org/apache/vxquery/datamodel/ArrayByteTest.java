package org.apache.vxquery.datamodel;

import java.io.IOException;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonItem.ArrayPointable;
import org.apache.vxquery.datamodel.builders.jsonItem.ArrayBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.junit.Test;

import junit.framework.Assert;

public class ArrayByteTest extends AbstractPointableTest {
    ArrayBackedValueStorage abvsResult = new ArrayBackedValueStorage();
    ArrayBuilder ab = new ArrayBuilder();
    TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp3 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();

    @Test
    public void testEmptyArrayConstant() {
        XDMConstants.setEmptyArray(tvp);
        if (tvp.getTag() != ValueTag.ARRAY_TAG) {
            Assert.fail("Type tag is incorrect. Expected: " + ValueTag.ARRAY_TAG + " Got: " + tvp.getTag());
        }
        tvp.getValue(ap);
        if (ap.getEntryCount() != 0) {
            Assert.fail("Array size is incorrect. Expected: 0 Got: " + ap.getEntryCount());
        }
    }

    @Test
    public void testEmptyArray() {
        // Build array sequence
        try {
            ab.reset(abvsResult);
            ab.finish();
        } catch (IOException e) {
            Assert.fail("Test failed to write the array pointable.");
        }
        tvp.set(abvsResult);

        // Check results.
        if (tvp.getTag() != ValueTag.ARRAY_TAG) {
            Assert.fail("Type tag is incorrect. Expected: " + ValueTag.ARRAY_TAG + " Got: " + tvp.getTag());
        }
        tvp.getValue(ap);
        if (ap.getEntryCount() != 0) {
            Assert.fail("Array size is incorrect. Expected: 0 Got: " + ap.getEntryCount());
        }
    }

    @Test
    public void testSingleItemArray() {
        // Build array sequence
        try {
            ab.reset(abvsResult);
            getTaggedValuePointable(1, tvp1);
            ab.addItem(tvp1);
            ab.finish();
        } catch (IOException e) {
            Assert.fail("Test failed to write the array pointable.");
        }
        tvp.set(abvsResult);

        // Check results.
        if (tvp.getTag() != ValueTag.ARRAY_TAG) {
            Assert.fail("Type tag is incorrect. Expected: " + ValueTag.ARRAY_TAG + " Got: " + tvp.getTag());
        }
        tvp.getValue(ap);
        if (ap.getEntryCount() != 1) {
            Assert.fail("Array size is incorrect. Expected: 1 Got: " + ap.getEntryCount());
        }
        ap.getEntry(0, tvp);
        if (!comparePointable(tvp, tvp1)) {
            Assert.fail("Array item is incorrect. Expected: " + ValueTag.XS_LONG_TAG + " Got: " + tvp.getTag());
        }

    }

    @Test
    public void testManyItemsArray() {
        // Build test array
        try {
            // Add three items
            ab.reset(abvsResult);
            getTaggedValuePointable(1, tvp1);
            ab.addItem(tvp1);
            getTaggedValuePointable(2.0, tvp2);
            ab.addItem(tvp2);
            getTaggedValuePointable("three", tvp3);
            ab.addItem(tvp3);
            ab.finish();
        } catch (IOException e) {
            Assert.fail("Test failed to write the array pointable.");
        }
        tvp.set(abvsResult);

        // Check results.
        if (tvp.getTag() != ValueTag.ARRAY_TAG) {
            Assert.fail("Array tag is incorrect. Expected: " + ValueTag.ARRAY_TAG + " Got: " + tvp.getTag());
        }
        tvp.getValue(ap);
        if (ap.getEntryCount() != 3) {
            Assert.fail("Array size is incorrect. Expected: 3 Got: " + ap.getEntryCount());
        }
        ap.getEntry(0, tvp);
        if (!comparePointable(tvp, tvp1)) {
            Assert.fail("Array item one is incorrect. Expected: " + ValueTag.XS_LONG_TAG + " Got: " + tvp.getTag());
        }
        ap.getEntry(1, tvp);
        if (!comparePointable(tvp, tvp2)) {
            Assert.fail("Array item two is incorrect. Expected: " + ValueTag.XS_DOUBLE_TAG + " Got: " + tvp.getTag());
        }
        ap.getEntry(2, tvp);
        if (!comparePointable(tvp, tvp3)) {
            Assert.fail("Array item three is incorrect. Expected: " + ValueTag.XS_STRING_TAG + " Got: " + tvp.getTag());
        }
    }

}
