/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.hive.writers;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.hive.writers.complex.HiveListWriter;
import org.apache.drill.exec.store.hive.writers.complex.HiveMapWriter;
import org.apache.drill.exec.store.hive.writers.complex.HiveStructWriter;
import org.apache.drill.exec.store.hive.writers.complex.HiveUnionWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveBinaryWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveBooleanWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveByteWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveCharWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveDateWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveDecimalWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveDoubleWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveFloatWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveIntWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveLongWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveShortWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveStringWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveTimestampWriter;
import org.apache.drill.exec.store.hive.writers.primitive.HiveVarCharWriter;
import org.apache.drill.exec.vector.complex.impl.SingleMapWriter;
import org.apache.drill.exec.vector.complex.impl.UnionWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.DateWriter;
import org.apache.drill.exec.vector.complex.writer.Float4Writer;
import org.apache.drill.exec.vector.complex.writer.Float8Writer;
import org.apache.drill.exec.vector.complex.writer.IntWriter;
import org.apache.drill.exec.vector.complex.writer.TimeStampWriter;
import org.apache.drill.exec.vector.complex.writer.VarBinaryWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.apache.drill.exec.vector.complex.writer.VarDecimalWriter;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.drill.exec.store.hive.HiveUtilities.throwUnsupportedHiveDataTypeError;

/**
 * Major component used in HiveExperimentalReader, which determines which writer should be used for
 * writing Hive column row data into .
 */
public final class HiveValueWriterFactory {

  private static final Logger logger = LoggerFactory.getLogger(HiveValueWriterFactory.class);

  /**
   * One buffer used to create all writers with current factory instance
   */
  private final DrillBuf drillBuf;

  /**
   * The writer is serves as a factory for concrete vector writers
   * used to handle data of written by each concrete {@link HiveValueWriter}
   * created in the factory.
   */
  private final SingleMapWriter rootWriter;

  public HiveValueWriterFactory(DrillBuf drillBuf, SingleMapWriter rootWriter) {
    this.drillBuf = drillBuf;
    this.rootWriter = rootWriter;
  }

  /**
   * Method that will be called once for each column in reader to initialize
   * writer for each one, by doing this we will have not fully dynamic data reading
   * like in {@link org.apache.drill.exec.store.easy.json.JSONRecordReader}, but more
   * predefined vector writers based on Hive metadata.
   *
   * @param columnName name of column for writer
   * @param fieldRef   metadata about field type
   * @return instance of writer for column
   */
  public HiveValueWriter createHiveColumnValueWriter(String columnName, StructField fieldRef) {
    ObjectInspector objectInspector = fieldRef.getFieldObjectInspector();
    final TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(objectInspector.getTypeName());
    return createHiveColumnValueWriter(columnName, typeInfo, objectInspector, rootWriter);
  }

  private HiveValueWriter createHiveColumnValueWriter(String columnName, TypeInfo typeInfo, ObjectInspector objectInspector, BaseWriter parentWriter) {

    switch (typeInfo.getCategory()) {
      case PRIMITIVE:
        return createPrimitiveColumnValueWriter(columnName, objectInspector, (PrimitiveTypeInfo) typeInfo, parentWriter);
      case LIST: {
        ListWriter listWriter = extractWriter(columnName, parentWriter, MapWriter::list, ListWriter::list);
        ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
        TypeInfo elemTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
        ObjectInspector elementInspector = listObjectInspector.getListElementObjectInspector();
        HiveValueWriter elementValueWriter = createHiveColumnValueWriter(null, elemTypeInfo, elementInspector, listWriter);
        return new HiveListWriter(listObjectInspector, listWriter, elementValueWriter);
      }
      case MAP: {
        // todo: this is how the setup code will look after implementation of 'real' map vector
        MapWriter mapWriter = extractWriter(columnName, parentWriter, MapWriter::map, ListWriter::map);
        MapObjectInspector mapObjectInspector = (MapObjectInspector) objectInspector;
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        TypeInfo keyTypeInfo = mapTypeInfo.getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();

        HiveValueWriter mapKeyWriter = createHiveColumnValueWriter(null, keyTypeInfo, mapObjectInspector.getMapKeyObjectInspector(), mapWriter);
        HiveValueWriter mapValueWriter = createHiveColumnValueWriter(null, valueTypeInfo, mapObjectInspector.getMapValueObjectInspector(), mapWriter);
        return new HiveMapWriter(mapObjectInspector, mapWriter, mapKeyWriter, mapValueWriter);
      }
      case STRUCT: {
        StructObjectInspector structObjectInspector = (StructObjectInspector) objectInspector;
        StructField[] structFields = structObjectInspector.getAllStructFieldRefs().toArray(new StructField[0]);
        HiveValueWriter[] structFieldWriters = new HiveValueWriter[structFields.length];

        MapWriter structWriter = extractWriter(columnName, parentWriter, MapWriter::map, ListWriter::map);
        for (int fieldIdx = 0; fieldIdx < structFields.length; fieldIdx++) {
          StructField field = structFields[fieldIdx];
          ObjectInspector fieldObjectInspector = field.getFieldObjectInspector();
          TypeInfo fieldTypeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fieldObjectInspector.getTypeName());
          structFieldWriters[fieldIdx] = createHiveColumnValueWriter(field.getFieldName(), fieldTypeInfo, fieldObjectInspector, structWriter);
        }

        return new HiveStructWriter(structObjectInspector, structFields, structFieldWriters);
      }
      case UNION: {
        UnionObjectInspector unionObjectInspector = (UnionObjectInspector) objectInspector;
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> unionFieldsTypeInfo = unionTypeInfo.getAllUnionObjectTypeInfos();
        List<ObjectInspector> objectInspectors = unionObjectInspector.getObjectInspectors();
        UnionWriter unionWriter = null; // todo: use extract writer method

        HiveValueWriter[] hiveUnionFieldWriters = new HiveValueWriter[unionFieldsTypeInfo.size()];
        for (int tag = 0; tag < unionFieldsTypeInfo.size(); tag++) {
          ObjectInspector unionFieldInspector = objectInspectors.get(tag);
          TypeInfo unionFieldTypeInfo = unionFieldsTypeInfo.get(tag);
          HiveValueWriter unionValueWriter = createHiveColumnValueWriter(null, unionFieldTypeInfo, unionFieldInspector, unionWriter);
          hiveUnionFieldWriters[tag] = unionValueWriter;
        }
        return new HiveUnionWriter(hiveUnionFieldWriters, unionObjectInspector);
      }
    }
    throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    return null;
  }

  /**
   * Links column with declared primitive type to instance of {@link HiveValueWriter}
   *
   * @param name      column name
   * @param inspector inspector of column values
   * @param typeInfo  column type used to distinguish returned writers
   * @return appropriate instance of HiveValueWriter for column containing primitive scalar
   */
  private HiveValueWriter createPrimitiveColumnValueWriter(String name, ObjectInspector inspector, PrimitiveTypeInfo typeInfo, BaseWriter parentWriter) {
    switch (typeInfo.getPrimitiveCategory()) {
      case BINARY: {
        VarBinaryWriter writer = extractWriter(name, parentWriter, MapWriter::varBinary, ListWriter::varBinary);
        return new HiveBinaryWriter((BinaryObjectInspector) inspector, writer, drillBuf);
      }
      case BOOLEAN: {
        BitWriter writer = extractWriter(name, parentWriter, MapWriter::bit, ListWriter::bit);
        return new HiveBooleanWriter((BooleanObjectInspector) inspector, writer);
      }
      case BYTE: {
        IntWriter writer = extractWriter(name, parentWriter, MapWriter::integer, ListWriter::integer);
        return new HiveByteWriter((ByteObjectInspector) inspector, writer);
      }
      case DOUBLE: {
        Float8Writer writer = extractWriter(name, parentWriter, MapWriter::float8, ListWriter::float8);
        return new HiveDoubleWriter((DoubleObjectInspector) inspector, writer);
      }
      case FLOAT: {
        Float4Writer writer = extractWriter(name, parentWriter, MapWriter::float4, ListWriter::float4);
        return new HiveFloatWriter((FloatObjectInspector) inspector, writer);
      }
      case INT: {
        IntWriter writer = extractWriter(name, parentWriter, MapWriter::integer, ListWriter::integer);
        return new HiveIntWriter((IntObjectInspector) inspector, writer);
      }
      case LONG: {
        BigIntWriter writer = extractWriter(name, parentWriter, MapWriter::bigInt, ListWriter::bigInt);
        return new HiveLongWriter((LongObjectInspector) inspector, writer);
      }
      case SHORT: {
        IntWriter writer = extractWriter(name, parentWriter, MapWriter::integer, ListWriter::integer);
        return new HiveShortWriter((ShortObjectInspector) inspector, writer);
      }
      case STRING: {
        VarCharWriter writer = extractWriter(name, parentWriter, MapWriter::varChar, ListWriter::varChar);
        return new HiveStringWriter((StringObjectInspector) inspector, writer, drillBuf);
      }
      case VARCHAR: {
        VarCharWriter writer = extractWriter(name, parentWriter, MapWriter::varChar, ListWriter::varChar);
        return new HiveVarCharWriter((HiveVarcharObjectInspector) inspector, writer, drillBuf);
      }
      case TIMESTAMP: {
        TimeStampWriter writer = extractWriter(name, parentWriter, MapWriter::timeStamp, ListWriter::timeStamp);
        return new HiveTimestampWriter((TimestampObjectInspector) inspector, writer);
      }
      case DATE: {
        DateWriter writer = extractWriter(name, parentWriter, MapWriter::date, ListWriter::date);
        return new HiveDateWriter((DateObjectInspector) inspector, writer);
      }
      case CHAR: {
        VarCharWriter writer = extractWriter(name, parentWriter, MapWriter::varChar, ListWriter::varChar);
        return new HiveCharWriter((HiveCharObjectInspector) inspector, writer, drillBuf);
      }
      case DECIMAL: {
        DecimalTypeInfo decimalType = (DecimalTypeInfo) typeInfo;
        int scale = decimalType.getScale();
        int precision = decimalType.getPrecision();
        VarDecimalWriter writer = extractWriter(name, parentWriter,
            (mapWriter, key) -> mapWriter.varDecimal(key, scale, precision),
            listWriter -> listWriter.varDecimal(scale, precision));
        return new HiveDecimalWriter((HiveDecimalObjectInspector) inspector, writer, scale);
      }
      default:
        throw UserException.unsupportedError()
            .message("Unsupported primitive data type %s")
            .build(logger);
    }
  }

  /**
   * Used to extract child writer from parent writer, assuming that parent writer may be instance of
   * {@link MapWriter} or {@link ListWriter}
   *
   * @param name
   * @param parentWriter
   * @param fromMap
   * @param fromList
   * @param <T>
   * @return
   */
  private static <T> T extractWriter(String name, BaseWriter parentWriter, BiFunction<MapWriter, String, T> fromMap, Function<ListWriter, T> fromList) {
    if (parentWriter instanceof MapWriter && name != null) {
      return fromMap.apply((MapWriter) parentWriter, name);
    } else if (parentWriter instanceof ListWriter) {
      return fromList.apply((ListWriter) parentWriter);
    } else {
      throw new IllegalStateException(String.format("Parent writer with type [%s] is unsupported", parentWriter.getClass()));
    }
  }

}
