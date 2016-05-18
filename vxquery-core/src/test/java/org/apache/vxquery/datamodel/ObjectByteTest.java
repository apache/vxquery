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
    private ArrayBackedValueStorage abvsResult = new ArrayBackedValueStorage();
    private ObjectBuilder ob = new ObjectBuilder();
    private TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private TaggedValuePointable tvpKey1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private TaggedValuePointable tvpValue1 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private TaggedValuePointable tvpKey2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private TaggedValuePointable tvpValue2 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private TaggedValuePointable tvpKey3 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private TaggedValuePointable tvpValue3 = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private SequencePointable sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    private ObjectPointable op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();

    @Test
    public void testEmptyObject() {
        // Build test Object
        abvsResult.reset();
        try {
            ob.reset(abvsResult);
            ob.finish();
        } catch (IOException e) {
            Assert.fail("Test failed to write the Object pointable.");
        }
        tvp.set(abvsResult);

        // Check results.
        if (tvp.getTag() != ValueTag.OBJECT_TAG) {
            Assert.fail("Type tag is incorrect. Expected: " + ValueTag.OBJECT_TAG + " Got: " + tvp.getTag());
        }
        tvp.getValue(op);
        if (op.getEntryCount() != 0) {
            Assert.fail("Object size is incorrect. Expected: 0 Got: " + op.getEntryCount());
        }
    }

    @Test
    public void testSingleItemObject() {
        // Build test Object
        abvsResult.reset();
        try {
            ob.reset(abvsResult);
            getTaggedValuePointable("id", tvpKey1);
            getTaggedValuePointable(1, tvpValue1);
            ob.addItem(tvpKey1, tvpValue1);
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
        if (op.getEntryCount() != 1) {
            Assert.fail("Object size is incorrect. Expected: 1 Got: " + op.getEntryCount());
        }
        try {
            op.getKeys(tvp);
        } catch (SystemException e) {
            Assert.fail("Test failed to write the object pointable.");
        }

        if (tvp.getTag() != ValueTag.XS_STRING_TAG) {
            Assert.fail("Type tag is incorrect. Expected: " + ValueTag.XS_STRING_TAG + " Got: " + tvp.getTag());
        }
        if (!FunctionHelper.arraysEqual(tvp, tvpKey1)) {
            Assert.fail("Key is incorrect. Expected: id");
        }
        try {
            op.getValue(tvpKey1, tvp);
        } catch (SystemException e) {
            Assert.fail("Test failed to write the object pointable.");
        }
        if (!FunctionHelper.arraysEqual(tvp, tvpValue1)) {
            Assert.fail("Value is incorrect for the given key.");
        }
    }

    @Test
    public void testManyItemObject() {
        // Build test object
        try {
            // Add three items
            ob.reset(abvsResult);
            getTaggedValuePointable("name", tvpKey1);
            getTaggedValuePointable("A green door", tvpValue1);
            ob.addItem(tvpKey1, tvpValue1);
            getTaggedValuePointable("price", tvpKey2);
            getTaggedValuePointable(12.5, tvpValue2);
            ob.addItem(tvpKey2, tvpValue2);
            getTaggedValuePointable("properties", tvpKey3);
            getTaggedValuePointable(100L, tvpValue3);
            ob.addItem(tvpKey3, tvpValue3);
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
            Assert.fail("Tag type is incorrect. Expected: " + ValueTag.SEQUENCE_TAG + " Got: " + tvp.getTag());
        }
        tvp.getValue(sp);
        if (sp.getEntryCount() != 3) {
            Assert.fail("Object size is incorrect. Expected: 3 Got: " + sp.getEntryCount());
        }
        sp.getEntry(0, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvpKey1)) {
            Assert.fail("Object key one is incorrect. Expected: name");
        }
        sp.getEntry(1, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvpKey2)) {
            Assert.fail("Object key two is incorrect. Expected: price");
        }
        sp.getEntry(2, tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvpKey3)) {
            Assert.fail("Object key three is incorrect. Expected: properties");
        }

        //Test values
        try {
            op.getValue(tvpKey1, tvp);
        } catch (SystemException e) {
            Assert.fail("Test failed to write the object pointable.");
        }
        if (!FunctionHelper.arraysEqual(tvp, tvpValue1)) {
            Assert.fail("Value is incorrect for the given key. Expected: A green door with valuetag: "
                    + ValueTag.XS_STRING_TAG + " Got valuetag: " + tvp.getTag());
        }
        try {
            op.getValue(tvpKey2, tvp);
        } catch (SystemException e) {
            Assert.fail("Test failed to write the object pointable.");
        }
        if (!FunctionHelper.arraysEqual(tvp, tvpValue2)) {
            Assert.fail("Value is incorrect for the given key. Expected: 12.5 with valuetag: " + ValueTag.XS_DOUBLE_TAG
                    + " Got valuetag: " + tvp.getTag());
        }
        try {
            op.getValue(tvpKey3, tvp);
        } catch (SystemException e) {
            Assert.fail("Test failed to write the object pointable.");
        }
        if (!FunctionHelper.arraysEqual(tvp, tvpValue3)) {
            Assert.fail("Value is incorrect for the given key. Expected: 100 with valuetag: " + ValueTag.XS_LONG_TAG
                    + " Got valuetag: " + tvp.getTag());
        }
    }

    @Test
    public void testItemNotExistObject() {
        // Build empty test Object
        abvsResult.reset();
        try {
            ob.reset(abvsResult);
            ob.finish();
        } catch (IOException e) {
            Assert.fail("Test failed to write the Object pointable.");
        }
        tvp.set(abvsResult);
        tvp.getValue(op);
        try {
            getTaggedValuePointable("key", tvpKey1);
            op.getValue(tvpKey1, tvpValue1);
        } catch (IOException | SystemException e) {
            Assert.fail("Test failed to write the object pointable.");
        }
        XDMConstants.setFalse(tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvpValue1)) {
            Assert.fail("Value is incorrect for the given key. Expected: false with valuetag: "
                    + ValueTag.XS_BOOLEAN_TAG + " Got valuetag: " + tvp.getTag());
        }

        // Build test object
        try {
            // Add two items
            ob.reset(abvsResult);
            getTaggedValuePointable("name", tvpKey1);
            getTaggedValuePointable("A green door", tvpValue1);
            ob.addItem(tvpKey1, tvpValue1);
            getTaggedValuePointable("price", tvpKey2);
            getTaggedValuePointable(12.5, tvpValue2);
            ob.addItem(tvpKey2, tvpValue2);
            ob.finish();
        } catch (IOException e) {
            Assert.fail("Test failed to write the object pointable.");
        }

        tvp.set(abvsResult);
        tvp.getValue(op);
        try {
            getTaggedValuePointable("key", tvpKey3);
            op.getValue(tvpKey3, tvpValue3);
        } catch (IOException | SystemException e) {
            Assert.fail("Test failed to write the object pointable.");
        }
        XDMConstants.setFalse(tvp);
        if (!FunctionHelper.arraysEqual(tvp, tvpValue3)) {
            Assert.fail("Value is incorrect for the given key. Expected: false with valuetag: "
                    + ValueTag.XS_BOOLEAN_TAG + " Got valuetag: " + tvp.getTag());
        }
    }
}
