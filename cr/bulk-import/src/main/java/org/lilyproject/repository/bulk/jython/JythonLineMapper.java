/*
 * Copyright 2012 NGDATA nv
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
package org.lilyproject.repository.bulk.jython;

import java.io.PrintWriter;
import java.io.Writer;

import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.bulk.LineMapper;
import org.lilyproject.repository.bulk.LineMappingContext;
import org.python.core.Py;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

/**
 * Maps single input records (lines of text) into Lily {@link Record} objects using a user-supplied callable.
 * <p>
 * The signature of the callable is as follows:
 * 
 * <pre>
 * __call__(inputLine, mappingContext)
 * </pre>
 * 
 * The <tt>inputLine</tt> argument is a single string, with line-ending characters removed. The <tt>mappingContext</tt>
 * argument is a {@link LineMappingContext} object which can be used to create records and ids, as well as writing
 * created records.
 * <p>
 * The callable can write zero, one, or many {@code Record} objects to the context.
 * <p>
 * The callable must be indentifiable as a single symbol. In the case of a global function, this is just the name of a
 * function. If the callable is a method in a class, an instance of the class must be created and put in the global
 * scope.
 * <p>
 * An example of a global function would be something like the following:
 * 
 * <pre>
 *  def myMapFunction(inputLine, mappingContext):
 *      rec = mappingContext.newRecord()
 *      rec.setRecordType(ctx.qn("{org.lilyproject}Person"))
 *      firstName, lastName = inputLine.split(",")
 *      rec.setField("{org.lilyproject}FirstName", firstName)
 *      rec.setField("{org.lilyproject}LastName", lastName)
 *      mappingContext.writeRecord(rec)
 * </pre>
 * 
 * The mapping function in the above example would be provided as "myMapFunction".
 * <p>
 * Using a method on an instance of the class can be done as follows:
 * 
 * <pre>
 *  class MyMapperClass(object):
 *      def myMapMethod(self, inputLine, mappingContext):
 *          rec = mappingContext.newRecord()
 *          rec.setRecordType(ctx.qn("{org.lilyproject}Person"))
 *          firstName, lastName = inputLine.split(",")
 *          rec.setField("{org.lilyproject}FirstName", firstName)
 *          rec.setField("{org.lilyproject}LastName", lastName)
 *          mappingContext.writeRecord(rec)
 * 
 *  mapMethod = MyMapperClass().myMapMethod
 * </pre>
 * 
 * The mapping function in this example would be provided as "mapMethod".
 */
public class JythonLineMapper implements LineMapper {
    private PythonInterpreter interpreter;
    private PyObject mapperCallable;

    public JythonLineMapper(String pythonCode, String recordMapperSymbol) {
        this(pythonCode, recordMapperSymbol, new PrintWriter(System.err));
    }

    public JythonLineMapper(String pythonCode, String recordMapperSymbol, Writer stderrWriter) {
        interpreter = new PythonInterpreter();
        interpreter.setErr(stderrWriter);

        // TODO Should we (or can we) restrict what can be done here?
        interpreter.exec(pythonCode);
        mapperCallable = interpreter.get(recordMapperSymbol);
        if (mapperCallable == null) {
            throw new IllegalArgumentException("Symbol " + recordMapperSymbol + " cannot be found");
        }
    }

    @Override
    public void mapLine(String inputLine, LineMappingContext context) {
        mapperCallable.__call__(new PyString(inputLine), Py.java2py(context));
    }

}
