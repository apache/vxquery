/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.vxquery.datamodel;

import java.io.IOException;

import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonItem.ObjectPointable;
import org.apache.vxquery.datamodel.builders.jsonItem.ObjectBuilder;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.datamodel.values.XDMConstants;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.util.FunctionHelper;
import org.junit.Test;

import junit.framework.Assert;

public class ObjectByteTest extends AbstractPointableTest {
    ArrayBackedValueStorage abvsResult = new ArrayBackedValueStorage();
    ObjectBuilder ob = new ObjectBuilder();
    TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp3 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp4 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp5 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    TaggedValuePointable tvp6 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    SequencePointable sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    ObjectPointable op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();

    @Test
    public void testEmptyObject() {
        // Build test Object
        abvsResult.reset();
        try {
            ob.reset(abvsResult);
            ob.finish();
        } catch (IOException e) {
            Assert.fail("Test failed to write the sequence pointable.");
        }
        tvp.set(abvsResult);

        // Check results.
        if (tvp.getTag() != ValueTag.OBJECT_TAG) {
            Assert.fail("Type tag is incorrect. Expected: " + ValueTag.OBJECT_TAG + " Got: " + tvp.getTag());
        }
        tvp.getValue(op);
        if (op.getEntryCount() != 0) {
            Assert.fail("Sequence size is incorrect. Expected: 0 Got: " + op.getEntryCount());
        }
    }

    @Test
    public void testSingleItemObject() {
        // Build test Object
        abvsResult.reset();
        try {
            ob.reset(abvsResult);
            getTaggedValuePointable("id", tvp1);
            getTaggedValuePointable(1, tvp2);
            ob.addItem(tvp1, tvp2);
            ob.finish();
        } catch (IOException e) {
            Assert.fail("Test failed to write the object pointable.");
        }
        tvp.set(abvsResult);

        // Check results.
        if (tvp.getTag() != ValueTag.OBJECT_TAG) {
            Assert.fail("Type tag is incorrect. Expected: " + ValueTag.OBJECT_TAG + " Got: " + tvp.getTag());
        }

        tvp.getValue(op);
        try {
            op.getKeys(tvp);
        } catch (SystemException e) {
            Assert.fail("Test failed to write the object pointable.");
        }

        if (tvp.getTag() != ValueTag.XS_STRING_TAG) {
            Assert.fail("Type tag is incorrect. Expected: " + ValueTag.XS_STRING_TAG + " Got: " + tvp.getTag());
        }
        if (!FunctionHelper.arraysEqual(tvp, tvp1)) {
            Assert.fail("Key is incorrect.");
        }

        op.getValue(tvp1, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvp2)) {
            Assert.fail("Value is incorrect for the given key.");
        }
    }

    @Test
    public void testManyItemObject() {
        // Build test object
        try {
            // Add three items
            ob.reset(abvsResult);
            getTaggedValuePointable("name", tvp1);
            getTaggedValuePointable("A green door", tvp2);
            ob.addItem(tvp1, tvp2);
            getTaggedValuePointable("price", tvp3);
            getTaggedValuePointable(12.5, tvp4);
            ob.addItem(tvp3, tvp4);
            getTaggedValuePointable("properties", tvp5);
            getTaggedValuePointable(100l, tvp6);
            ob.addItem(tvp5, tvp6);
            ob.finish();
        } catch (IOException e) {
            Assert.fail("Test failed to write the object pointable.");
        }
        tvp.set(abvsResult);

        // Check results.
        if (tvp.getTag() != ValueTag.OBJECT_TAG) {
            Assert.fail("Type tag is incorrect. Expected: " + ValueTag.SEQUENCE_TAG + " Got: " + tvp.getTag());
        }
        tvp.getValue(op);
        if (op.getEntryCount() != 3) {
            Assert.fail("Object size is incorrect. Expected: 3 Got: " + op.getEntryCount());
        }

        //Test keys
        try {
            op.getKeys(tvp);
        } catch (SystemException e) {
            Assert.fail("Test failed to write the object pointable.");
        }

        if (tvp.getTag() != ValueTag.SEQUENCE_TAG) {
            Assert.fail("Sequence tag is incorrect. Expected: " + ValueTag.SEQUENCE_TAG + " Got: " + tvp.getTag());
        }
        tvp.getValue(sp);
        if (sp.getEntryCount() != 3) {
            Assert.fail("Sequence size is incorrect. Expected: 3 Got: " + sp.getEntryCount());
        }
        sp.getEntry(0, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvp1)) {
            Assert.fail("Sequence item one is incorrect. Expected: " + ValueTag.XS_LONG_TAG + " Got: " + tvp.getTag());
        }
        sp.getEntry(1, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvp3)) {
            Assert.fail(
                    "Sequence item two is incorrect. Expected: " + ValueTag.XS_DOUBLE_TAG + " Got: " + tvp.getTag());
        }
        sp.getEntry(2, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvp5)) {
            Assert.fail(
                    "Sequence item three is incorrect. Expected: " + ValueTag.XS_STRING_TAG + " Got: " + tvp.getTag());
        }

        //Test values
        op.getValue(tvp1, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvp2)) {
            Assert.fail("Value is incorrect for the given key.");
        }
        op.getValue(tvp3, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvp4)) {
            Assert.fail("Value is incorrect for the given key.");
        }
        op.getValue(tvp5, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvp6)) {
            Assert.fail("Value is incorrect for the given key.");
        }
    }

}
