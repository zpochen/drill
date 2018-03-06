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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.ValueExpressions.*;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.kudu.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KuduCompareFunctionsProcessor extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
  static final Logger logger = LoggerFactory
          .getLogger(KuduCompareFunctionsProcessor.class);
  private Object value;
  private boolean success;
  private boolean isEqualityFn;
  private SchemaPath path;
  private String functionName;

  public static boolean isCompareFunction(String functionName) {
    return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
  }

  public static KuduCompareFunctionsProcessor process(FunctionCall call) {
    String functionName = call.getName();
    LogicalExpression nameArg = call.args.get(0);
    LogicalExpression valueArg = call.args.size() >= 2 ? call.args.get(1) : null;
    KuduCompareFunctionsProcessor evaluator = new KuduCompareFunctionsProcessor(functionName);

    if (valueArg != null) { // binary function
      if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
        LogicalExpression swapArg = valueArg;
        valueArg = nameArg;
        nameArg = swapArg;
        evaluator.functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName);
      }
      evaluator.success = nameArg.accept(evaluator, valueArg);
    } else if (call.args.get(0) instanceof SchemaPath) {
      evaluator.success = true;
      evaluator.path = (SchemaPath) nameArg;
    }

    return evaluator;
  }

  public KuduCompareFunctionsProcessor(String functionName) {
    this.success = false;
    this.functionName = functionName;
    this.isEqualityFn = COMPARE_FUNCTIONS_TRANSPOSE_MAP.containsKey(functionName)
            && COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName).equals(functionName);
  }

  public Object getValue() {
    return value;
  }

  public boolean isSuccess() {
    return success;
  }

  public SchemaPath getPath() {
    return path;
  }

  public String getFunctionName() {
    return functionName;
  }

  @Override
  public Boolean visitCastExpression(CastExpression e, LogicalExpression valueArg) throws RuntimeException {
    if (e.getInput() instanceof CastExpression || e.getInput() instanceof SchemaPath) {
      return e.getInput().accept(this, valueArg);
    }
    return false;
  }

  @Override
  public Boolean visitConvertExpression(ConvertExpression e, LogicalExpression valueArg) throws RuntimeException {
    if (e.getConvertFunction() == ConvertExpression.CONVERT_FROM
            && e.getInput() instanceof SchemaPath) {
      String encodingType = e.getEncodingType();
      switch (encodingType) {
        case "INT_BE":
        case "INT":
        case "UINT_BE":
        case "UINT":
        case "UINT4_BE":
        case "UINT4":
          if (valueArg instanceof IntExpression
                  && (isEqualityFn || encodingType.startsWith("U"))) {
            this.value = ((IntExpression) valueArg).getInt();
          }
          break;
        case "BIGINT_BE":
        case "BIGINT":
        case "UINT8_BE":
        case "UINT8":
          if (valueArg instanceof LongExpression
                  && (isEqualityFn || encodingType.startsWith("U"))) {
            this.value = ((LongExpression) valueArg).getLong();
          }
          break;
        case "FLOAT":
          if (valueArg instanceof FloatExpression && isEqualityFn) {
            this.value = ((FloatExpression) valueArg).getFloat();
          }
          break;
        case "DOUBLE":
          if (valueArg instanceof DoubleExpression && isEqualityFn) {
            this.value = ((DoubleExpression) valueArg).getDouble();
          }
          break;
        case "TIME_EPOCH":
        case "TIME_EPOCH_BE":
          if (valueArg instanceof TimeExpression) {
            this.value = ((TimeExpression) valueArg).getTime();
          }
          break;
        case "DATE_EPOCH":
        case "DATE_EPOCH_BE":
          if (valueArg instanceof DateExpression) {
            this.value = ((DateExpression) valueArg).getDate();
          }
          break;
        case "BOOLEAN_BYTE":
          if (valueArg instanceof BooleanExpression) {
            this.value = ((BooleanExpression) valueArg).getBoolean();
          }
          break;
        case "UTF8":
          // let visitSchemaPath() handle this.
          return e.getInput().accept(this, valueArg);

      }

      if (value != null) {
        this.path = (SchemaPath) e.getInput();
        return true;
      }
    }
    return false;
  }


  @Override
  public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg) throws RuntimeException {
    return false;
  }

  @Override
  public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg)
          throws RuntimeException {
    if (valueArg instanceof QuotedString) {
      this.value = ((QuotedString) valueArg).value;
      this.path = path;
      return true;
    }

    if (valueArg instanceof IntExpression) {
      this.value = ((IntExpression) valueArg).getInt();
      this.path = path;
      return true;
    }

    if (valueArg instanceof LongExpression) {
      this.value = ((LongExpression) valueArg).getLong();
      this.path = path;
      return true;
    }

    if (valueArg instanceof FloatExpression) {
      this.value = ((FloatExpression) valueArg).getFloat();
      this.path = path;
      return true;
    }

    if (valueArg instanceof DoubleExpression) {
      this.value = ((DoubleExpression) valueArg).getDouble();
      this.path = path;
      return true;
    }

    if (valueArg instanceof BooleanExpression) {
      this.value = ((BooleanExpression) valueArg).getBoolean();
      this.path = path;
      return true;
    }

    return false;
  }


  public static Type getType(Object value){
    if (value instanceof Integer){
      return Type.INT32;
    }else if (value instanceof Long){
      return Type.INT64;
    }else if (value instanceof String){
      return Type.STRING;
    }else if (value instanceof Boolean){
      return Type.BOOL;
    }
    return Type.STRING;
  }


  private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;

  static {
    ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet.builder();
    VALUE_EXPRESSION_CLASSES = builder
            .add(BooleanExpression.class)
            .add(DateExpression.class)
            .add(DoubleExpression.class)
            .add(FloatExpression.class)
            .add(IntExpression.class)
            .add(LongExpression.class)
            .add(QuotedString.class)
            .add(TimeExpression.class)
            .build();
  }

  private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;

  static {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
            // unary functions
            .put("isnotnull", "isnotnull")
            .put("isNotNull", "isNotNull")
            .put("is not null", "is not null")
            .put("isnull", "isnull")
            .put("isNull", "isNull")
            .put("is null", "is null")
            // binary functions
            .put("like", "like")
            .put("equal", "equal")
            .put("not_equal", "not_equal")
            .put("greater_than_or_equal_to", "less_than_or_equal_to")
            .put("greater_than", "less_than")
            .put("less_than_or_equal_to", "greater_than_or_equal_to")
            .put("less_than", "greater_than")
            .build();
  }

}
