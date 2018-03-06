/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.kudu;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kudu.client.KuduPredicate;

import java.util.List;

public class KuduScanSpec {

  private final String tableName;

  private List<KuduPredicate> filters;

  @JsonCreator
  public KuduScanSpec(@JsonProperty("tableName") String tableName) {
    this.tableName = tableName;
  }

  public KuduScanSpec(String tableName, List<KuduPredicate> filters) {
    this.tableName = tableName;
    this.filters = filters;
  }

  public String getTableName() {
    return tableName;
  }

  public List<KuduPredicate> getFilters(){
    return filters;
  }

}
