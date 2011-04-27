/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.repository.avro;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.ipc.generic.GenericResponder;
import org.apache.avro.specific.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

// This code is copied from the Avro codebase with some customisations to handle
// remote propagation of runtime exceptions

public class LilySpecificResponder extends GenericResponder {
  private Object impl;
  private SpecificData data;
    private AvroConverter converter;
    private Log log = LogFactory.getLog(getClass());

  public LilySpecificResponder(Class iface, Object impl, AvroConverter converter) {
    this(SpecificData.get().getProtocol(iface), impl);
      this.converter = converter;
  }

  public LilySpecificResponder(Protocol protocol, Object impl) {
    this(protocol, impl, SpecificData.get());
  }

  protected LilySpecificResponder(Protocol protocol, Object impl,
                              SpecificData data) {
    super(protocol);
    this.impl = impl;
    this.data = data;
  }

  @Override
  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new SpecificDatumWriter<Object>(schema);
  }

  @Override
  protected DatumReader<Object> getDatumReader(Schema actual, Schema expected) {
    return new SpecificDatumReader<Object>(actual, expected);
  }

  @Override
  public void writeError(Schema schema, Object error,
                         Encoder out) throws IOException {
    getDatumWriter(schema).write(error, out);
  }

  @Override
  public Object respond(Protocol.Message message, Object request) throws Exception {
    int numParams = message.getRequest().getFields().size();
    Object[] params = new Object[numParams];
    Class[] paramTypes = new Class[numParams];
    int i = 0;
    try {
      for (Schema.Field param: message.getRequest().getFields()) {
        params[i] = ((GenericRecord)request).get(param.name());
        paramTypes[i] = data.getClass(param.schema());
        i++;
      }
      Method method = impl.getClass().getMethod(message.getName(), paramTypes);
      method.setAccessible(true);
      return method.invoke(impl, params);
    } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof SpecificRecord) {
            throw (Exception)e.getTargetException();
        } else {
            throw converter.convertOtherException(e.getTargetException());
        }
    } catch (NoSuchMethodException e) {
      throw new AvroRuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

}