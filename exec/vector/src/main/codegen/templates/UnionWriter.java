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

import org.apache.drill.common.types.TypeProtos;

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/UnionWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

public class UnionWriter extends AbstractFieldWriter implements FieldWriter {

  // Accessed by UnionReader
  protected UnionVector data;
  private StructWriter structWriter;
  private UnionListWriter listWriter;
  private List<BaseWriter> writers = Lists.newArrayList();

  public UnionWriter(BufferAllocator allocator) {
    super(null);
  }

  public UnionWriter(UnionVector vector) {
    super(null);
    data = vector;
  }

  public UnionWriter(UnionVector vector, FieldWriter parent) {
    super(null);
    data = vector;
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (BaseWriter writer : writers) {
      writer.setPosition(index);
    }
  }

  @Override
  public void start() {
    data.getMutator().setType(idx(), MinorType.STRUCT);
    getStructWriter().start();
  }

  @Override
  public void end() {
    getStructWriter().end();
  }

  @Override
  public void startList() {
    getListWriter().startList();
    data.getMutator().setType(idx(), MinorType.LIST);
  }

  @Override
  public void endList() {
    getListWriter().endList();
  }

  private StructWriter getStructWriter() {
    if (structWriter == null) {
      structWriter = new SingleStructWriter(data.getStruct(), null, true);
      structWriter.setPosition(idx());
      writers.add(structWriter);
    }
    return structWriter;
  }

  public StructWriter asStruct() {
    data.getMutator().setType(idx(), MinorType.STRUCT);
    return getStructWriter();
  }

  private ListWriter getListWriter() {
    if (listWriter == null) {
      listWriter = new UnionListWriter(data.getList());
      listWriter.setPosition(idx());
      writers.add(listWriter);
    }
    return listWriter;
  }

  public ListWriter asList() {
    data.getMutator().setType(idx(), MinorType.LIST);
    return getListWriter();
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>

          <#if !minor.class?starts_with("Decimal")>

  private ${name}Writer ${name?uncap_first}Writer;

  private ${name}Writer get${name}Writer() {
    if (${uncappedName}Writer == null) {
      ${uncappedName}Writer = new Nullable${name}WriterImpl(data.get${name}Vector(), null);
      ${uncappedName}Writer.setPosition(idx());
      writers.add(${uncappedName}Writer);
    }
    return ${uncappedName}Writer;
  }

  public ${name}Writer as${name}() {
    data.getMutator().setType(idx(), MinorType.${name?upper_case});
    return get${name}Writer();
  }

  @Override
  public void write(${name}Holder holder) {
    data.getMutator().setType(idx(), MinorType.${name?upper_case});
    get${name}Writer().setPosition(idx());
    get${name}Writer().write${name}(<#list fields as field>holder.${field.name}<#if field_has_next>, </#if></#list>);
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    data.getMutator().setType(idx(), MinorType.${name?upper_case});
    get${name}Writer().setPosition(idx());
    get${name}Writer().write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
  }
  </#if>
  </#list></#list>

  public void writeNull() { }

  @Override
  public StructWriter struct() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().struct();
  }

  @Override
  public ListWriter list() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().list();
  }

  @Override
  public ListWriter list(String name) {
    data.getMutator().setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().list(name);
  }

  @Override
  public StructWriter struct(String name) {
    data.getMutator().setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().struct(name);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#if minor.class == "VarDecimal">
  @Override
  public ${capName}Writer ${lowerName}(String name, int scale, int precision) {
    data.getMutator().setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().${lowerName}(name, scale, precision);
  }

  @Override
  public ${capName}Writer ${lowerName}(int scale, int precision) {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().${lowerName}(scale, precision);
  }
  <#else>
  @Override
  public ${capName}Writer ${lowerName}(String name) {
    data.getMutator().setType(idx(), MinorType.STRUCT);
    getStructWriter().setPosition(idx());
    return getStructWriter().${lowerName}(name);
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter().setPosition(idx());
    return getListWriter().${lowerName}();
  }
  </#if>
  </#list></#list>

  @Override
  public void allocate() {
    data.allocateNew();
  }

  @Override
  public void clear() {
    data.clear();
  }

  @Override
  public void close() throws Exception {
    data.close();
  }

  @Override
  public MaterializedField getField() {
    return data.getField();
  }

  @Override
  public int getValueCapacity() {
    return data.getValueCapacity();
  }
}
