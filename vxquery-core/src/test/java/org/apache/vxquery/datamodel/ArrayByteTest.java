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

import java.io.IOException;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.builders.jsonitem.ArrayBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.junit.Assert;
import org.junit.Test;

public class ArrayByteTest extends AbstractPointableTest {
    ArrayBackedValueStorage abvsResult = new ArrayBackedValueStorage();
    ArrayBuilder ab = new ArrayBuilder();
    TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp3 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();

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
        if (!FunctionHelper.arraysEqual(tvp, tvp1)) {
            Assert.fail("Array item is incorrect. Expected: " + ValueTag.XS_INT_TAG + " Got: " + tvp.getTag());
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
        if (!FunctionHelper.arraysEqual(tvp, tvp1)) {
            Assert.fail("Array item one is incorrect. Expected: " + ValueTag.XS_INT_TAG + " Got: " + tvp.getTag());
        }
        ap.getEntry(1, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvp2)) {
            Assert.fail("Array item two is incorrect. Expected: " + ValueTag.XS_DOUBLE_TAG + " Got: " + tvp.getTag());
        }
        ap.getEntry(2, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvp3)) {
            Assert.fail("Array item three is incorrect. Expected: " + ValueTag.XS_STRING_TAG + " Got: " + tvp.getTag());
        }
    }
}
