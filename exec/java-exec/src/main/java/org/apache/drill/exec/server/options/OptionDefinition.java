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
package org.apache.drill.exec.server.options;

import org.apache.drill.exec.ExecOpt;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

/**
 * This holds all the information about an option.
 */
public class OptionDefinition {
  private final OptionValidator validator;
  private final OptionMetaData metaData;

  private OptionDefinition(OptionValidator validator, OptionMetaData metaData) {
    this.validator = Preconditions.checkNotNull(validator);
    this.metaData = Preconditions.checkNotNull(metaData);
  }

  public OptionValidator getValidator() {
    return validator;
  }

  public OptionMetaData getMetaData() {
    return metaData;
  }

  public static Builder defBuilder(ExecOpt opt) {
    return new Builder(opt.validator);
  }

  public static Builder defBuilder(OptionValidator validator) {
    return new Builder(validator);
  }

  public static OptionDefinition def(OptionValidator validator) {
    return defBuilder(validator).build();
  }

  public static OptionDefinition def(ExecOpt opt) {
    return defBuilder(opt).build();
  }

  public static class Builder {
    private final OptionValidator validator;
    private OptionMetaData optMeta;

    public Builder(OptionValidator validator) {
      this.validator = validator;
      this.optMeta = OptionMetaData.DEFAULT;
    }

    public Builder scopes(OptionValue.AccessibleScopes scopes) {
      if (optMeta.getAccessibleScopes() != scopes) {
        optMeta = new OptionMetaData(scopes, optMeta.isAdminOnly(), optMeta.isInternal());
      }
      return this;
    }

    public Builder adminOnly() {
      if (!optMeta.isAdminOnly()) {
        optMeta = new OptionMetaData(optMeta.getAccessibleScopes(),
            true, optMeta.isInternal());
      }
      return this;
    }

    public Builder internal() {
      if (!optMeta.isInternal()) {
        optMeta = new OptionMetaData(optMeta.getAccessibleScopes(),
            optMeta.isAdminOnly(), true);
      }
      return this;
    }

    public OptionDefinition build() {
      return new OptionDefinition(validator, optMeta);
    }
  }
}
