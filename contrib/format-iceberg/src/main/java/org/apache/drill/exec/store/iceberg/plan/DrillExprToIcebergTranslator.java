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
package org.apache.drill.exec.store.iceberg.plan;

import org.apache.drill.common.FunctionNames;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.exec.store.iceberg.IcebergGroupScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

public class DrillExprToIcebergTranslator extends AbstractExprVisitor<Expression, Void, RuntimeException> {

  public static final ExprVisitor<Expression, Void, RuntimeException> INSTANCE = new DrillExprToIcebergTranslator();

  @Override
  public Expression visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
    switch (call.getName()) {
      case FunctionNames.AND: {
        Expression left = call.arg(0).accept(this, null);
        Expression right = call.arg(1).accept(this, null);
        if (left != null && right != null) {
          return Expressions.and(left, right);
        }
        return null;
      }
      case FunctionNames.OR: {
        Expression left = call.arg(0).accept(this, null);
        Expression right = call.arg(1).accept(this, null);
        if (left != null && right != null) {
          return Expressions.or(left, right);
        }
        return null;
      }
      case FunctionNames.NOT: {
        Expression expression = call.arg(0).accept(this, null);
        if (expression != null) {
          return Expressions.not(expression);
        }
        return null;
      }
      case FunctionNames.IS_NULL: {
        LogicalExpression arg = call.arg(0);
        if (arg instanceof SchemaPath) {
          String name = IcebergGroupScan.getPath((SchemaPath) arg);
          return Expressions.isNull(name);
        }
        return null;
      }
      case FunctionNames.IS_NOT_NULL: {
        LogicalExpression arg = call.arg(0);
        if (arg instanceof SchemaPath) {
          String name = IcebergGroupScan.getPath((SchemaPath) arg);
          return Expressions.notNull(name);
        }
        return null;
      }
      case FunctionNames.LT: {
        LogicalExpression nameRef = call.arg(0);
        Expression expression = call.arg(1).accept(this, null);
        if (nameRef instanceof SchemaPath && expression instanceof ConstantExpression) {
          String name = IcebergGroupScan.getPath((SchemaPath) nameRef);
          return Expressions.lessThan(name, ((ConstantExpression<?>) expression).getValue());
        }
        return null;
      }
      case FunctionNames.LE: {
        LogicalExpression nameRef = call.arg(0);
        Expression expression = call.arg(1).accept(this, null);
        if (nameRef instanceof SchemaPath && expression instanceof ConstantExpression) {
          String name = IcebergGroupScan.getPath((SchemaPath) nameRef);
          return Expressions.lessThanOrEqual(name, ((ConstantExpression<?>) expression).getValue());
        }
        return null;
      }
      case FunctionNames.GT: {
        LogicalExpression nameRef = call.args().get(0);
        Expression expression = call.args().get(1).accept(this, null);
        if (nameRef instanceof SchemaPath && expression instanceof ConstantExpression) {
          String name = IcebergGroupScan.getPath((SchemaPath) nameRef);
          return Expressions.greaterThan(name, ((ConstantExpression<?>) expression).getValue());
        }
        return null;
      }
      case FunctionNames.GE: {
        LogicalExpression nameRef = call.args().get(0);
        Expression expression = call.args().get(1).accept(this, null);
        if (nameRef instanceof SchemaPath && expression instanceof ConstantExpression) {
          String name = IcebergGroupScan.getPath((SchemaPath) nameRef);
          return Expressions.greaterThanOrEqual(name, ((ConstantExpression<?>) expression).getValue());
        }
        return null;
      }
      case FunctionNames.EQ: {
        LogicalExpression nameRef = call.args().get(0);
        Expression expression = call.args().get(1).accept(this, null);
        if (nameRef instanceof SchemaPath && expression instanceof ConstantExpression) {
          String name = IcebergGroupScan.getPath((SchemaPath) nameRef);
          return Expressions.equal(name, ((ConstantExpression<?>) expression).getValue());
        }
        return null;
      }
      case FunctionNames.NE: {
        LogicalExpression nameRef = call.args().get(0);
        Expression expression = call.args().get(1).accept(this, null);
        if (nameRef instanceof SchemaPath && expression instanceof ConstantExpression) {
          String name = IcebergGroupScan.getPath((SchemaPath) nameRef);
          return Expressions.notEqual(name, ((ConstantExpression<?>) expression).getValue());
        }
        return null;
      }
    }
    return null;
  }

  @Override
  public Expression visitFloatConstant(ValueExpressions.FloatExpression fExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(fExpr.getFloat());
  }

  @Override
  public Expression visitIntConstant(ValueExpressions.IntExpression intExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(intExpr.getInt());
  }

  @Override
  public Expression visitLongConstant(ValueExpressions.LongExpression longExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(longExpr.getLong());
  }

  @Override
  public Expression visitDecimal9Constant(ValueExpressions.Decimal9Expression decExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(decExpr.getIntFromDecimal());
  }

  @Override
  public Expression visitDecimal18Constant(ValueExpressions.Decimal18Expression decExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(decExpr.getLongFromDecimal());
  }

  @Override
  public Expression visitDecimal28Constant(ValueExpressions.Decimal28Expression decExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(decExpr.getBigDecimal());
  }

  @Override
  public Expression visitDecimal38Constant(ValueExpressions.Decimal38Expression decExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(decExpr.getBigDecimal());
  }

  @Override
  public Expression visitVarDecimalConstant(ValueExpressions.VarDecimalExpression decExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(decExpr.getBigDecimal());
  }

  @Override
  public Expression visitDateConstant(ValueExpressions.DateExpression dateExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(Math.toIntExact(Math.floorDiv(dateExpr.getDate(), 86400000L)));
  }

  @Override
  public Expression visitCastExpression(CastExpression cast, Void value) {
    if (cast.getMajorType().getMinorType() == org.apache.drill.common.types.TypeProtos.MinorType.TIME) {
      try {
        int millis;
        if (cast.getInput() instanceof ValueExpressions.IntExpression) {
          millis = ((ValueExpressions.IntExpression) cast.getInput()).getInt();
        } else if (cast.getInput() instanceof ValueExpressions.LongExpression) {
          millis = Math.toIntExact(((ValueExpressions.LongExpression) cast.getInput()).getLong());
        } else if (cast.getInput() instanceof ValueExpressions.QuotedString) {
          millis = (int) (LocalTime.parse(((ValueExpressions.QuotedString) cast.getInput())
              .getString()).toNanoOfDay() / 1_000_000);
        } else {
          return null;
        }
        return visitTimeConstant(new ValueExpressions.TimeExpression(millis), value);
      } catch (DateTimeParseException | ArithmeticException e) {
        return null;
      }
    }
    if (cast.getMajorType().getMinorType() == org.apache.drill.common.types.TypeProtos.MinorType.TIMESTAMP) {
      try {
        long millis;
        if (cast.getInput() instanceof ValueExpressions.LongExpression) {
          millis = ((ValueExpressions.LongExpression) cast.getInput()).getLong();
        } else if (cast.getInput() instanceof ValueExpressions.IntExpression) {
          millis = ((ValueExpressions.IntExpression) cast.getInput()).getInt();
        } else if (cast.getInput() instanceof ValueExpressions.QuotedString) {
          millis = LocalDateTime.parse(((ValueExpressions.QuotedString) cast.getInput())
              .getString().replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli();
        } else {
          return null;
        }
        return visitTimeStampConstant(new ValueExpressions.TimeStampExpression(millis), value);
      } catch (DateTimeParseException | ArithmeticException e) {
        return null;
      }
    }
    // Reconstruct epoch days after a minor fragment deserializes a DATE cast.
    if (cast.getMajorType().getMinorType() == org.apache.drill.common.types.TypeProtos.MinorType.DATE) {
      if (cast.getInput() instanceof ValueExpressions.LongExpression) {
        return visitDateConstant(new ValueExpressions.DateExpression(
            ((ValueExpressions.LongExpression) cast.getInput()).getLong()), value);
      }
      if (cast.getInput() instanceof ValueExpressions.IntExpression) {
        return visitDateConstant(new ValueExpressions.DateExpression(
            ((ValueExpressions.IntExpression) cast.getInput()).getInt()), value);
      }
      if (!(cast.getInput() instanceof ValueExpressions.QuotedString)) {
        return null;
      }
      try {
        return new ConstantExpression<>(Math.toIntExact(LocalDate.parse(
            ((ValueExpressions.QuotedString) cast.getInput()).getString()).toEpochDay()));
      } catch (DateTimeParseException e) {
        return null;
      }
    }
    return null;
  }

  @Override
  public Expression visitTimeConstant(ValueExpressions.TimeExpression timeExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(timeExpr.getTime() * 1000L);
  }

  @Override
  public Expression visitTimeStampConstant(ValueExpressions.TimeStampExpression timestampExpr, Void value) throws RuntimeException {
    try {
      return new ConstantExpression<>(Math.multiplyExact(timestampExpr.getTimeStamp(), 1000L));
    } catch (ArithmeticException e) {
      return null;
    }
  }

  @Override
  public Expression visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, Void value) throws RuntimeException {
    return new ConstantExpression<>(dExpr.getDouble());
  }

  @Override
  public Expression visitBooleanConstant(ValueExpressions.BooleanExpression e, Void value) throws RuntimeException {
    return new ConstantExpression<>(e.getBoolean());
  }

  @Override
  public Expression visitQuotedStringConstant(ValueExpressions.QuotedString e, Void value) throws RuntimeException {
    return new ConstantExpression<>(e.getString());
  }

  @Override
  public Expression visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    return null;
  }

  private static class ConstantExpression<T> implements Expression {
    private final T value;

    public ConstantExpression(T value) {
      this.value = value;
    }

    @Override
    public Operation op() {
      return null;
    }

    public T getValue() {
      return value;
    }
  }
}
