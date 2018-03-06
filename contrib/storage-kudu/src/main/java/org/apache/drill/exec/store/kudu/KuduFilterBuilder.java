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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class KuduFilterBuilder extends AbstractExprVisitor<KuduScanSpec, Void, RuntimeException> {
  static final Logger logger = LoggerFactory
          .getLogger(KuduFilterBuilder.class);

  final private KuduGroupScan groupScan;

  final private LogicalExpression le;

  private boolean allExpressionsConverted = true;

  private static Boolean nullComparatorSupported = null;

  KuduFilterBuilder(KuduGroupScan groupScan, LogicalExpression le) {
    this.groupScan = groupScan;
    this.le = le;
  }

  public KuduScanSpec parseTree() {
    KuduScanSpec parsedSpec = le.accept(this, null);
    if (parsedSpec != null) {
      parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getKuduScanSpec(), parsedSpec);
    }
    return parsedSpec;
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public KuduScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    allExpressionsConverted = false;
    return null;
  }

  @Override
  public KuduScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
    return visitFunctionCall(op, value);
  }

  @Override
  public KuduScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    KuduScanSpec nodeScanSpec = null;
    String functionName = call.getName();
    ImmutableList<LogicalExpression> args = call.args;

    if (KuduCompareFunctionsProcessor.isCompareFunction(functionName)) {
      KuduCompareFunctionsProcessor processor = KuduCompareFunctionsProcessor.process(call);
      if (processor.isSuccess()) {
        try {
          nodeScanSpec = createKuduScanSpec(call, processor);
        } catch (Exception e) {
          logger.error(" Failed to create Filter ", e);
          // throw new RuntimeException(e.getMessage(), e);
        }
      }
    } else {
      switch (functionName) {
        case "booleanAnd":
        case "booleanOr":
          KuduScanSpec firstScanSpec = args.get(0).accept(this, null);
          for (int i = 1; i < args.size(); ++i) {
            KuduScanSpec nextScanSpec = args.get(i).accept(this, null);
            if (firstScanSpec != null && nextScanSpec != null) {
              nodeScanSpec = mergeScanSpecs(functionName, firstScanSpec, nextScanSpec);
            } else {
              allExpressionsConverted = false;
              if ("booleanAnd".equals(functionName)) {
                nodeScanSpec = firstScanSpec == null ? nextScanSpec : firstScanSpec;
              }
            }
            firstScanSpec = nodeScanSpec;
          }
          break;
      }
    }

    if (nodeScanSpec == null) {
      allExpressionsConverted = false;
    }

    return nodeScanSpec;
  }

  private KuduScanSpec mergeScanSpecs(String functionName,
                                      KuduScanSpec leftScanSpec, KuduScanSpec rightScanSpec) {
    List<KuduPredicate> newFilters = null;

    switch (functionName) {
      case "booleanAnd":
        if (leftScanSpec.getFilters() != null
                && rightScanSpec.getFilters() != null) {
          newFilters = Lists.newArrayList(Iterables.concat(leftScanSpec.getFilters(), rightScanSpec.getFilters()));
        } else if (leftScanSpec.getFilters() != null) {
          newFilters = leftScanSpec.getFilters();
        } else {
          newFilters = rightScanSpec.getFilters();
        }
        break;
      case "booleanOr"://TODO kudu not surport or condition operator
        if (leftScanSpec.getFilters() != null) {
          newFilters = leftScanSpec.getFilters();
        } else {
          newFilters = rightScanSpec.getFilters();
        }
    }
    return new KuduScanSpec(groupScan.getTableName(), newFilters);
  }

  private KuduScanSpec createKuduScanSpec(FunctionCall call, KuduCompareFunctionsProcessor processor) {
    String functionName = processor.getFunctionName();
    SchemaPath field = processor.getPath();
    Object fieldValue = processor.getValue();

    String fieldName = field.getRootSegmentPath();
    KuduPredicate.ComparisonOp compareOp = null;
    Boolean isnull = false;
    Boolean isnotnull = false;
    switch (functionName) {
      case "equal":
        compareOp = KuduPredicate.ComparisonOp.EQUAL;
        break;
      case "greater_than_or_equal_to":
        compareOp = KuduPredicate.ComparisonOp.GREATER_EQUAL;
        break;
      case "greater_than":
        compareOp = KuduPredicate.ComparisonOp.GREATER;
        break;
      case "less_than_or_equal_to":
        compareOp = KuduPredicate.ComparisonOp.LESS_EQUAL;
        break;
      case "less_than":
        compareOp = KuduPredicate.ComparisonOp.LESS;
        break;
      case "isnull":
      case "isNull":
      case "is null":
        isnull = true;
        break;
      case "isnotnull":
      case "isNotNull":
      case "is not null":
        isnotnull = true;
        break;
    }

    Type columnType = KuduCompareFunctionsProcessor.getType(processor.getValue());
    ColumnSchema column = new ColumnSchema.ColumnSchemaBuilder(fieldName, columnType).build();

    KuduPredicate predicate = null;
    if (compareOp != null) {
      if (columnType == Type.INT32) {
        predicate = KuduPredicate.newComparisonPredicate(column, compareOp, (Integer) fieldValue);
      } else if (columnType == Type.INT64) {
        predicate = KuduPredicate.newComparisonPredicate(column, compareOp, (Long) fieldValue);
      } else if (columnType == Type.STRING) {
        predicate = KuduPredicate.newComparisonPredicate(column, compareOp, (String) fieldValue);
      } else if (columnType == Type.BOOL) {
        predicate = KuduPredicate.newComparisonPredicate(column, compareOp, (Boolean) fieldValue);
      }else if (columnType == Type.FLOAT) {
        predicate = KuduPredicate.newComparisonPredicate(column, compareOp, (Float) fieldValue);
      }else if (columnType == Type.DOUBLE) {
        predicate = KuduPredicate.newComparisonPredicate(column, compareOp, (Double) fieldValue);
      }
    } else if (isnull) {
      predicate = KuduPredicate.newIsNullPredicate(column);
    } else if (isnotnull) {
      predicate = KuduPredicate.newIsNotNullPredicate(column);
    }

    if (predicate != null) {
      return new KuduScanSpec(groupScan.getTableName(), Arrays.asList(predicate));
    }
    return null;
  }

}
