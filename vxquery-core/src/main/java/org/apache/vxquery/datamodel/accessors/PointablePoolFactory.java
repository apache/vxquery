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
package org.apache.vxquery.datamodel.accessors;

import org.apache.vxquery.datamodel.accessors.atomic.CodedQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSBinaryPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDatePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDateTimePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDecimalPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSDurationPointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSQNamePointable;
import org.apache.vxquery.datamodel.accessors.atomic.XSTimePointable;
<<<<<<< HEAD
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
=======
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
>>>>>>> 80efee30c7bf002420a1036ff7f3fee891e32f44
import org.apache.vxquery.datamodel.accessors.nodes.AttributeNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.DocumentNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.ElementNodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.NodeTreePointable;
import org.apache.vxquery.datamodel.accessors.nodes.PINodePointable;
import org.apache.vxquery.datamodel.accessors.nodes.TextOrCommentNodePointable;

import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.FloatPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.ShortPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;

public class PointablePoolFactory {
    public static final PointablePoolFactory INSTANCE = new PointablePoolFactory();

    private PointablePoolFactory() {
    }

    public PointablePool createPointablePool() {
        PointablePool pp = new PointablePool();

        pp.register(TaggedValuePointable.class, TaggedValuePointable.FACTORY);
        pp.register(BooleanPointable.class, BooleanPointable.FACTORY);
        pp.register(BytePointable.class, BytePointable.FACTORY);
        pp.register(ShortPointable.class, ShortPointable.FACTORY);
        pp.register(IntegerPointable.class, IntegerPointable.FACTORY);
        pp.register(LongPointable.class, LongPointable.FACTORY);
        pp.register(FloatPointable.class, FloatPointable.FACTORY);
        pp.register(DoublePointable.class, DoublePointable.FACTORY);
        pp.register(UTF8StringPointable.class, UTF8StringPointable.FACTORY);
        pp.register(SequencePointable.class, SequencePointable.FACTORY);
        pp.register(VoidPointable.class, VoidPointable.FACTORY);
        pp.register(CodedQNamePointable.class, CodedQNamePointable.FACTORY);
        pp.register(XSBinaryPointable.class, XSBinaryPointable.FACTORY);
        pp.register(XSDatePointable.class, XSDatePointable.FACTORY);
        pp.register(XSDateTimePointable.class, XSDateTimePointable.FACTORY);
        pp.register(XSDecimalPointable.class, XSDecimalPointable.FACTORY);
        pp.register(XSDurationPointable.class, XSDurationPointable.FACTORY);
        pp.register(XSQNamePointable.class, XSQNamePointable.FACTORY);
        pp.register(XSTimePointable.class, XSTimePointable.FACTORY);

        pp.register(NodeTreePointable.class, NodeTreePointable.FACTORY);
        pp.register(DocumentNodePointable.class, DocumentNodePointable.FACTORY);
        pp.register(ElementNodePointable.class, ElementNodePointable.FACTORY);
        pp.register(ArrayPointable.class, ArrayPointable.FACTORY);
        pp.register(AttributeNodePointable.class, AttributeNodePointable.FACTORY);
        pp.register(TextOrCommentNodePointable.class, TextOrCommentNodePointable.FACTORY);
        pp.register(PINodePointable.class, PINodePointable.FACTORY);

        pp.register(ObjectPointable.class,ObjectPointable.FACTORY);

        return pp;
    }
}
