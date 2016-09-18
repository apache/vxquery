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
package org.apache.vxquery.runtime.functions.json;

import java.io.IOException;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.data.std.api.IPointable;
<<<<<<< 9f1b465c615e96008beb2f6ef02e530302b6bfe9
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.vxquery.datamodel.accessors.ArrayBackedValueStoragePool;
=======
>>>>>>> Implementation of PushValueIntoDatascanRule
import org.apache.vxquery.datamodel.accessors.PointablePool;
import org.apache.vxquery.datamodel.accessors.SequencePointable;
import org.apache.vxquery.datamodel.accessors.TaggedValuePointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ArrayPointable;
import org.apache.vxquery.datamodel.accessors.jsonitem.ObjectPointable;
import org.apache.vxquery.datamodel.values.ValueTag;
import org.apache.vxquery.exceptions.ErrorCode;
import org.apache.vxquery.exceptions.SystemException;
import org.apache.vxquery.runtime.functions.step.AbstractForwardAxisPathStep;

public class KeysOrMembersUnnesting extends AbstractForwardAxisPathStep {
<<<<<<< 9f1b465c615e96008beb2f6ef02e530302b6bfe9
	private final ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
	private final SequencePointable sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
	private final ObjectPointable op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
	private final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
	private final TaggedValuePointable tempTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
	private TaggedValuePointable arg = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
	private ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
	private final ArrayBackedValueStoragePool abvsPool = new ArrayBackedValueStoragePool();
	private int arOrObArgsLength;
	private int indexArrayArgs;

	public KeysOrMembersUnnesting(IHyracksTaskContext ctx, PointablePool pp) {
		super(ctx, pp);
	}

	protected void init(TaggedValuePointable[] args) throws SystemException {
		abvs = abvsPool.takeOne();
		indexArrayArgs = 0;
		arg = args[0];
		switch (arg.getTag()) {
		case ValueTag.OBJECT_TAG:
			arg.getValue(op);
			try {
				op.getKeys(abvs);
			} catch (IOException e) {
				throw new SystemException(ErrorCode.SYSE0001, e);
			}
			arOrObArgsLength = op.getEntryCount();
			break;
		case ValueTag.ARRAY_TAG:
			arg.getValue(ap);
			arOrObArgsLength = ap.getEntryCount();
			break;
		default:
			throw new SystemException(ErrorCode.FORG0006);
		}
	}

	public boolean step(IPointable result) {
		if (arOrObArgsLength > 0) {
			while (indexArrayArgs < arOrObArgsLength) {
				if (arg.getTag() == ValueTag.ARRAY_TAG) {
					ap.getEntry(indexArrayArgs, tvp);
				} else {
					tempTvp.getValue(sp);
					sp.getEntry(indexArrayArgs, tvp);
				}
				result.set(tvp.getByteArray(), tvp.getStartOffset(), tvp.getLength());
				indexArrayArgs++;
				return true;
			}
		}
		return false;
	}
=======
    private final ArrayPointable ap = (ArrayPointable) ArrayPointable.FACTORY.createPointable();
    private final SequencePointable sp = (SequencePointable) SequencePointable.FACTORY.createPointable();
    private final ObjectPointable op = (ObjectPointable) ObjectPointable.FACTORY.createPointable();
    private final TaggedValuePointable tvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private final TaggedValuePointable tempTvp = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    private TaggedValuePointable arg = (TaggedValuePointable) TaggedValuePointable.FACTORY.createPointable();
    //    private final ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
    private int arOrObArgsLength;
    private int indexArrayArgs;

    public KeysOrMembersUnnesting(IHyracksTaskContext ctx, PointablePool pp) {
        super(ctx, pp);
    }

    protected void init(TaggedValuePointable[] args) throws SystemException {
        indexArrayArgs = 0;
        arg = args[0];
        switch (arg.getTag()) {
            case ValueTag.OBJECT_TAG:
                arg.getValue(op);
                try {
                    op.getKeys(tempTvp);
                } catch (IOException e) {
                    throw new SystemException(ErrorCode.SYSE0001, e);
                }
                arOrObArgsLength = op.getEntryCount();
                break;
            case ValueTag.ARRAY_TAG:
                arg.getValue(ap);
                arOrObArgsLength = ap.getEntryCount();
                break;
            default:
                throw new SystemException(ErrorCode.FORG0006);
        }
    }

    public boolean step(IPointable result) {
        if (arOrObArgsLength > 0) {
            while (indexArrayArgs < arOrObArgsLength) {
                if (arg.getTag() == ValueTag.ARRAY_TAG) {
                    ap.getEntry(indexArrayArgs, tvp);
                } else {
                    tempTvp.getValue(sp);
                    sp.getEntry(indexArrayArgs, tvp);
                }
                result.set(tvp.getByteArray(), tvp.getStartOffset(), tvp.getLength());
                indexArrayArgs++;
                return true;
            }
        }
        return false;
    }
>>>>>>> Implementation of PushValueIntoDatascanRule
}
