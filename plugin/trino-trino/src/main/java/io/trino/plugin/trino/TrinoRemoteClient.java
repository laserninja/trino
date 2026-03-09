/*
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
package io.trino.plugin.trino;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.jdbc.Row;
import io.trino.jdbc.TrinoConnection;
import io.trino.jdbc.TrinoIntervalDayTime;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.ReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgDecimal;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCorr;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementRegrIntercept;
import io.trino.plugin.jdbc.aggregation.ImplementRegrSlope;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteCast;
import io.trino.plugin.jdbc.expression.RewriteDateConstant;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.expression.RewriteIntervalConstant;
import io.trino.plugin.jdbc.expression.RewriteTimestampConstant;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import java.sql.Array;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.JsonTypeUtil.jsonParse;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMappingUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.StandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.sql.Types.JAVA_OBJECT;
import static java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
import static java.sql.Types.TIME_WITH_TIMEZONE;
import static java.util.Objects.requireNonNull;

public class TrinoRemoteClient
            extends BaseJdbcClient
{
    private static final Logger log = Logger.get(TrinoRemoteClient.class);
    private final TypeManager typeManager;
    private final Type jsonType;
    private final Type intervalDayTimeType;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    private static final Pattern NESTED_DATA_TYPE_METADATA_ESCAPE_PATTERN = Pattern.compile("(\\s*)(?<fieldname>(\\w(\\.\\w)*)+)(?=(\\s+\\w))");
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;

    @Inject
    public TrinoRemoteClient(
            BaseJdbcConfig config,
            TrinoRemoteConfig trinoRemoteConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
            throws SQLException
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, queryModifier, true);
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.of(0), Optional.of(0), Optional.empty(), Optional.empty());
        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.intervalDayTimeType = typeManager.getType(new TypeSignature(INTERVAL_DAY_TO_SECOND));
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder()
                .addStandardRules(this::quoted)
                // TODO allow all comparison operators for numeric types
                .add(new RewriteIn())
                .add(new RewriteCast(this::mapTypeForCast))
                .add(new RewriteDateConstant())
                .add(new RewriteTimestampConstant())
                .add(new RewriteIntervalConstant(intervalDayTimeType))
                .withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint"))
                .withTypeClass("numeric_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint", "decimal", "real", "double"))
                .map("$equal(left, right)").to("left = right")
                .map("$not_equal(left, right)").to("left <> right")
                .map("$is_distinct_from(left, right)").to("left IS DISTINCT FROM right")
                .map("$less_than(left, right)").to("left < right")
                .map("$less_than_or_equal(left, right)").to("left <= right")
                .map("$greater_than(left, right)").to("left > right")
                .map("$greater_than_or_equal(left, right)").to("left >= right")
                .map("$add(left, right)").to("left + right")
                .map("$subtract(left, right)").to("left - right")
                .map("$multiply(left, right)").to("left * right")
                .map("$divide(left, right)").to("left / right")
                .map("$modulus(left, right)").to("left % right")
                .map("$negate(value)").to("-value")
                .map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern")
                .map("$like(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape")
                .map("$not($is_null(value))").to("value IS NOT NULL")
                .map("$not(value: boolean)").to("NOT value")
                .map("$is_null(value)").to("value IS NULL")
                .map("$nullif(first, second)").to("NULLIF(first, second)")
                .build();

        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(
                this.connectorExpressionRewriter,
                ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax(false))
                        .add(new ImplementCountDistinct(bigintTypeHandle, false))
                        .add(new ImplementSum(TrinoRemoteClient::convertType))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementStddevSamp())
                        .add(new ImplementStddevPop())
                        .add(new ImplementVarianceSamp())
                        .add(new ImplementVariancePop())
                        .add(new ImplementCovarianceSamp())
                        .add(new ImplementCovariancePop())
                        .add(new ImplementCorr())
                        .add(new ImplementRegrIntercept())
                        .add(new ImplementRegrSlope())
                        .build());
    }

    @Override
    public Optional<String> getTableComment(ResultSet resultSet)
    {
        try {
            return Optional.ofNullable(resultSet.getString("REMARKS"));
        }
        catch (SQLException ex) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<JdbcExpression> convertProjection(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments)
                .flatMap(parExp -> convertType(expression.getType()).map(type -> new JdbcExpression(parExp.expression(), parExp.parameters(), type)));
    }

    @Override
    public Optional<PreparedQuery> legacyImplementJoin(
            ConnectorSession session,
            JoinType joinType,
            PreparedQuery leftSource,
            PreparedQuery rightSource,
            List<JdbcJoinCondition> joinConditions,
            Map<JdbcColumnHandle, String> rightAssignments,
            Map<JdbcColumnHandle, String> leftAssignments,
            JoinStatistics statistics)
    {
        return super.legacyImplementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics);
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        return true;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of(TopNFunction.sqlStandard(this::quoted));
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex AggregateExpressions and functions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    private static Optional<JdbcTypeHandle> convertType(Type type)
    {
        requireNonNull(type, "type is null");
        String typeName = type.getBaseName();
        if (typeName.equals(StandardTypes.JSON)) {
            return Optional.of(getJDBCTypeHandle(Types.JAVA_OBJECT, Optional.of(typeName), Optional.empty(), Optional.empty(), Optional.empty()));
        }
        return switch (typeName) {
            case StandardTypes.BOOLEAN -> Optional.of(getJDBCTypeHandle(Types.BOOLEAN, Optional.of(typeName)));
            case StandardTypes.TINYINT -> Optional.of(getJDBCTypeHandle(Types.TINYINT, Optional.of(typeName)));
            case StandardTypes.SMALLINT -> Optional.of(getJDBCTypeHandle(Types.SMALLINT, Optional.of(typeName)));
            case StandardTypes.INTEGER -> Optional.of(getJDBCTypeHandle(Types.INTEGER, Optional.of(typeName)));
            case StandardTypes.BIGINT -> Optional.of(getJDBCTypeHandle(Types.BIGINT, Optional.of(typeName)));
            case StandardTypes.REAL -> Optional.of(getJDBCTypeHandle(Types.REAL, Optional.of(typeName)));
            case StandardTypes.DOUBLE -> Optional.of(getJDBCTypeHandle(Types.DOUBLE, Optional.of(typeName)));
            case StandardTypes.DECIMAL -> Optional.of(getJDBCTypeHandle(Types.NUMERIC, Optional.of(typeName), Optional.of(((DecimalType) type).getPrecision()), Optional.of(((DecimalType) type).getScale()), Optional.empty()));
            case StandardTypes.CHAR -> Optional.of(getJDBCTypeHandle(Types.CHAR, Optional.of(typeName), Optional.of(((CharType) type).getLength()), Optional.empty(), Optional.empty()));
            case StandardTypes.VARCHAR -> Optional.of(getJDBCTypeHandle(Types.VARCHAR, Optional.of(typeName), Optional.of(((VarcharType) type).getLength().orElse(Integer.MAX_VALUE)), Optional.empty(), Optional.empty()));
            case StandardTypes.VARBINARY -> Optional.of(getJDBCTypeHandle(Types.VARBINARY, Optional.of(typeName)));
            case StandardTypes.DATE -> Optional.of(getJDBCTypeHandle(Types.DATE, Optional.of(typeName)));
            case StandardTypes.TIME -> Optional.of(getJDBCTypeHandle(Types.TIME, Optional.of(typeName), Optional.of(((TimeType) type).getPrecision()), Optional.empty(), Optional.empty()));
            case StandardTypes.TIME_WITH_TIME_ZONE -> Optional.of(getJDBCTypeHandle(Types.TIME_WITH_TIMEZONE, Optional.of(typeName), Optional.of(((TimeWithTimeZoneType) type).getPrecision()), Optional.empty(), Optional.empty()));
            case StandardTypes.TIMESTAMP -> Optional.of(getJDBCTypeHandle(Types.TIMESTAMP, Optional.of(typeName), Optional.of(((TimestampType) type).getPrecision()), Optional.empty(), Optional.empty()));
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE -> Optional.of(getJDBCTypeHandle(Types.TIMESTAMP_WITH_TIMEZONE, Optional.of(typeName), Optional.of(((TimestampWithTimeZoneType) type).getPrecision()), Optional.empty(), Optional.empty()));
            case StandardTypes.ARRAY -> Optional.of(getJDBCTypeHandle(Types.ARRAY, Optional.of(((ArrayType) type).getElementType().getDisplayName()), Optional.empty(), Optional.empty(), Optional.empty()));
            default -> Optional.empty();
        };
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type.equals(this.jsonType)) {
            return WriteMapping.sliceMapping(StandardTypes.JSON, typedVarcharWriteFunction("json"));
        }
        else if (type.equals(this.intervalDayTimeType)) {
            return WriteMapping.longMapping(INTERVAL_DAY_TO_SECOND, trinoIntervalWriteMapping());
        }
        String typeName = type.getBaseName();
        return switch (typeName) {
            case StandardTypes.BOOLEAN -> WriteMapping.booleanMapping("boolean", booleanWriteFunction());
            case StandardTypes.TINYINT -> WriteMapping.longMapping("tinyint", tinyintWriteFunction());
            case StandardTypes.SMALLINT -> WriteMapping.longMapping("smallint", smallintWriteFunction());
            case StandardTypes.INTEGER -> WriteMapping.longMapping("integer", integerWriteFunction());
            case StandardTypes.BIGINT -> WriteMapping.longMapping("bigint", bigintWriteFunction());
            case StandardTypes.REAL -> WriteMapping.longMapping("float", realWriteFunction());
            case StandardTypes.DOUBLE -> WriteMapping.doubleMapping("double", doubleWriteFunction());
            case StandardTypes.DECIMAL -> {
                DecimalType decimalType = (DecimalType) type;
                String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
                if (decimalType.isShort()) {
                    yield WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
                }
                yield WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
            }
            case StandardTypes.CHAR -> WriteMapping.sliceMapping(format("char(%s)", ((CharType) type).getLength()), charWriteFunction());
            case StandardTypes.VARCHAR -> {
                String dataType;
                if (((VarcharType) type).isUnbounded()) {
                    dataType = "varchar";
                }
                else {
                    dataType = format("varchar(%s)", ((VarcharType) type).getBoundedLength());
                }
                yield WriteMapping.sliceMapping(dataType, varcharWriteFunction());
            }
            case StandardTypes.VARBINARY -> WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
            case StandardTypes.DATE -> WriteMapping.longMapping("date", dateWriteFunctionUsingLocalDate());
            case StandardTypes.TIME -> trinoTimeWriteMapping((TimeType) type);
            case StandardTypes.TIME_WITH_TIME_ZONE -> trinoTimeWithTimeZoneWriteMapping((TimeWithTimeZoneType) type);
            case StandardTypes.TIMESTAMP -> trinoTimestampWriteMapping((TimestampType) type);
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE -> trinoTimestampWithTimeZoneWriteMapping((TimestampWithTimeZoneType) type);
            case StandardTypes.ARRAY -> {
                Type elementType = ((ArrayType) type).getElementType();
                String elementDataType = toWriteMapping(session, elementType).getDataType();
                yield WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, elementType.getDisplayName()));
            }
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        };
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        if (typeHandle.jdbcTypeName().equals("json")) {
            return Optional.of(jsonColumnMapping());
        }
        switch (typeHandle.jdbcType()) {
            case Types.BIT, Types.BOOLEAN: return Optional.of(booleanColumnMapping());
            case Types.TINYINT: return Optional.of(tinyintColumnMapping());
            case Types.SMALLINT: return Optional.of(smallintColumnMapping());
            case Types.INTEGER: return Optional.of(integerColumnMapping());
            case Types.BIGINT: return Optional.of(bigintColumnMapping());
            case Types.REAL: return Optional.of(realColumnMapping());
            case Types.DOUBLE: return Optional.of(doubleColumnMapping());
            case Types.NUMERIC, Types.DECIMAL: {
                int decimalDigits = typeHandle.requiredDecimalDigits();
                int precision = typeHandle.requiredColumnSize();
                if (precision > Decimals.MAX_PRECISION) {
                    return Optional.empty();
                }
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0))));
            }
            case Types.CHAR, Types.NCHAR: return Optional.of(defaultCharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR: return Optional.of(defaultVarcharColumnMapping(typeHandle.requiredColumnSize(), true));
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY: return Optional.of(varbinaryColumnMapping());

            case Types.DATE: return Optional.of(dateColumnMappingUsingLocalDate());
            case Types.TIME: return Optional.of(timeColumnMapping(createTimeType(typeHandle.requiredDecimalDigits())));
            case Types.TIMESTAMP: return Optional.of(timestampColumnMapping(createTimestampType(typeHandle.requiredDecimalDigits())));
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                    return mapToUnboundedVarchar(typeHandle);
                }
                return Optional.empty();
            case Types.ARRAY: {
                JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
                String baseElementTypeName = baseElementTypeHandle.jdbcTypeName()
                        .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Array baseelement type name is missing: " + baseElementTypeHandle));
                return toColumnMapping(session, connection, baseElementTypeHandle)
                        .map(elementMapping -> {
                            ArrayType trinoArrayType = new ArrayType(elementMapping.getType());
                            return arrayColumnMapping(session, trinoArrayType, elementMapping, baseElementTypeName);
                        });
            }
            case Types.JAVA_OBJECT: return objectToTrinoType(session, jdbcTypeName);
            case Types.STRUCT: return rowToTrinoType(session, jdbcTypeName);
            default: {
                if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
                    return mapToUnboundedVarchar(typeHandle);
                }
                else {
                    log.debug("Trino-to-Trino Unsupported type: %s", typeHandle);
                    return Optional.empty();
                }
            }
        }
    }

    private JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String arrayTypeName = arrayTypeHandle.jdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing for jdbc type: " + JDBCType.valueOf(arrayTypeHandle.jdbcType())));
        // Trino Array elements are like this array(row(column1 varchar, column2 varchar)) - we need to extract the base element type - row(column1 varchar, column2 varchar)
        arrayTypeName = arrayTypeName.substring("array(".length(), arrayTypeName.length() - 1); // removing 'array(' and ')' and capture what is inside as the datatype
        arrayTypeName = escapeSpecialCharactersInJDBCTypeName(arrayTypeName); //
        return prepareJdbcTypeHandle(arrayTypeHandle, typeManager.fromSqlType(arrayTypeName).getTypeSignature().getBase(), arrayTypeName);
    }

    private Optional<ColumnMapping> objectToTrinoType(
            ConnectorSession session,
            String jdbcTypeName)
    {
        String quotedJdbcTypeName = escapeSpecialCharactersInJDBCTypeName(jdbcTypeName);
        Object objType = typeManager.fromSqlType(quotedJdbcTypeName);
        if (objType instanceof MapType) {
            return Optional.of(mapColumnMapping(session, (MapType) objType, quotedJdbcTypeName));
        }
        // By default, we are assuming the Objects are ROW types.
        return Optional.of(rowColumnMapping(session, (RowType) objType, quotedJdbcTypeName));
    }

    private String escapeSpecialCharactersInJDBCTypeName(String jdbcTypeName)
    {
        String quotedJdbcTypeName = replaceTokens(
                jdbcTypeName,
                NESTED_DATA_TYPE_METADATA_ESCAPE_PATTERN,
                match -> '"' + match.group("fieldname") + '"');
        return quotedJdbcTypeName;
    }

    private Optional<ColumnMapping> rowToTrinoType(
            ConnectorSession session,
            String jdbcTypeName)
    {
        String quotedJdbcTypeName = escapeSpecialCharactersInJDBCTypeName(jdbcTypeName);
        RowType rowType = (RowType) typeManager.fromSqlType(quotedJdbcTypeName);
        return Optional.of(rowColumnMapping(session, rowType, quotedJdbcTypeName));
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, ColumnMapping arrayElementMapping, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(arrayType.getElementType(), arrayElementMapping.getReadFunction()),
                arrayWriteFunction(session, arrayType.getElementType(), baseElementJdbcTypeName));
    }

    private static ObjectReadFunction arrayReadFunction(Type elementType, ReadFunction elementReadFunction)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Object[] objectArray = TypeUtils.toBoxedArray(resultSet.getArray(columnIndex).getArray());
            return TypeUtils.jdbcObjectArrayToBlock(elementType, objectArray);
        });
    }

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String elementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(elementJdbcTypeName, TypeUtils.getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        });
    }

    private static ColumnMapping mapColumnMapping(ConnectorSession session, MapType mapType, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                mapType,
                mapReadFunction(session, mapType),
                mapWriteFunction(session, mapType, baseElementJdbcTypeName));
    }

    private static ObjectReadFunction mapReadFunction(ConnectorSession session, MapType mapType)
    {
        return ObjectReadFunction.of(SqlMap.class, (resultSet, columnIndex) -> {
            Map<?, Object> mapData = (Map<?, Object>) resultSet.getObject(columnIndex);
            BlockBuilder keyBlockBuilder = mapType.getKeyType().createBlockBuilder(null, mapData.size());
            BlockBuilder valueBlockBuilder = mapType.getValueType().createBlockBuilder(null, mapData.size());

            for (Map.Entry<?, ?> entry : ((Map<?, ?>) mapData).entrySet()) {
                if (entry.getKey() == null) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Map record key is null");
                }
                TypeUtils.appendToBlockBuilder(mapType.getKeyType(), entry.getKey(), keyBlockBuilder);
                if (entry.getValue() == null) {
                    valueBlockBuilder.appendNull();
                }
                else {
                    TypeUtils.appendToBlockBuilder(mapType.getValueType(), entry.getValue(), valueBlockBuilder);
                }
            }
            MapBlock mapBlock = mapType.createBlockFromKeyValue(
                    Optional.empty(),
                    new int[] {0, mapData.size()},
                    keyBlockBuilder.build(),
                    valueBlockBuilder.build());
            return mapType.getObject(mapBlock, 0);
        });
    }

    private static ObjectWriteFunction mapWriteFunction(ConnectorSession session, MapType elementType, String elementJdbcTypeName)
    {
        return ObjectWriteFunction.of(SqlMap.class, (statement, index, sqlMap) -> {
            int rawOffset = sqlMap.getRawOffset();
            Block rawKeyBlock = sqlMap.getRawKeyBlock();
            Block rawValueBlock = sqlMap.getRawValueBlock();

            Type keyType = elementType.getKeyType();
            Type valueType = elementType.getValueType();
            Map<Object, Object> map = new HashMap<>();

            for (int i = 0; i < sqlMap.getSize(); i++) {
                map.put(keyType.getObjectValue(session, rawKeyBlock, rawOffset + i), valueType.getObjectValue(session, rawValueBlock, rawOffset + i));
            }
            statement.setObject(index, Collections.unmodifiableMap(map));
        });
    }

    private static ColumnMapping rowColumnMapping(ConnectorSession session, RowType rowType, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                rowType,
                rowReadFunction(session, rowType),
                rowWriteFunction(session, rowType, baseElementJdbcTypeName));
    }

    private static ObjectReadFunction rowReadFunction(ConnectorSession session, RowType rowType)
    {
        return ObjectReadFunction.of(SqlRow.class, (resultSet, columnIndex) -> {
            Row rowData = (Row) resultSet.getObject(columnIndex);
            Map<?, Object> mapRowData = rowData.getFields().stream()
                    .collect(HashMap::new, (m, v) -> m.put(v.getName().get(), v.getValue()), HashMap::putAll);
            verify(rowData.getFields().size() == rowType.getFields().size(), "Type mismatch: %s, %s", rowData, rowType);

            return buildRowValue(rowType, fieldBuilders -> {
                int fieldPosition = 0;
                for (RowType.Field field : rowType.getFields()) {
                    BlockBuilder fieldBuilder = fieldBuilders.get(fieldPosition);
                    Object fieldData = mapRowData.get(field.getName().get());
                    TypeUtils.appendToBlockBuilder(field.getType(), fieldData, fieldBuilder);
                    fieldPosition++;
                }
            });
        });
    }

    private static ObjectWriteFunction rowWriteFunction(ConnectorSession session, Type elementType, String elementJdbcTypeName)
    {
        return ObjectWriteFunction.of(SqlRow.class, (statement, index, sqlRow) -> {
            List<Object> values = new ArrayList<>(sqlRow.getFieldCount());
            RowType rowType = (RowType) elementType;
            int rawIndex = sqlRow.getRawIndex();
            for (int fieldIndex = 0; fieldIndex < sqlRow.getFieldCount(); fieldIndex++) {
                Type fieldType = rowType.getTypeParameters().get(fieldIndex);
                values.add(fieldType.getObjectValue(session, sqlRow.getRawFieldBlock(fieldIndex), rawIndex));
            }
//            sqlRow.getRawFieldBlocks();
//            Object objectValue = elementType.getObjectValue(session, sqlRow.getRawFieldBlocks(), )readNativeValue(elementType, block, index);
            //TODO: The below code may not work - we need to write the method that will write data as a STRUCT
            Object jdbcObject = statement.getConnection().createStruct(elementJdbcTypeName, values.toArray());
            statement.setObject(index, jdbcObject);
        });
    }

    private ColumnMapping jsonColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                typedVarcharWriteFunction("json"),
                DISABLE_PUSHDOWN);
    }

    private static JdbcTypeHandle prepareJdbcTypeHandle(JdbcTypeHandle original, String typeName, String elementTypeName)
    {
        return switch (typeName) {
            case "boolean" -> getJDBCTypeHandle(Types.BOOLEAN, Optional.ofNullable(typeName), original.arrayDimensions());
            case "bigint" -> getJDBCTypeHandle(Types.BIGINT, Optional.ofNullable(typeName), original.arrayDimensions());
            case "integer" -> getJDBCTypeHandle(Types.INTEGER, Optional.ofNullable(typeName), original.arrayDimensions());
            case "smallint" -> getJDBCTypeHandle(Types.SMALLINT, Optional.ofNullable(typeName), original.arrayDimensions());
            case "tinyint" -> getJDBCTypeHandle(Types.TINYINT, Optional.ofNullable(typeName), original.arrayDimensions());
            case "real" -> getJDBCTypeHandle(Types.REAL, Optional.ofNullable(typeName), original.arrayDimensions());
            case "double" -> getJDBCTypeHandle(Types.DOUBLE, Optional.ofNullable(typeName), original.arrayDimensions());
            case "varchar" -> getJDBCTypeHandle(Types.VARCHAR, Optional.ofNullable(typeName), Optional.of(original.columnSize().orElse(VarcharType.MAX_LENGTH)), Optional.empty(), original.arrayDimensions());
            case "char" -> getJDBCTypeHandle(Types.CHAR, Optional.ofNullable(typeName), Optional.of(original.columnSize().orElse(VarcharType.MAX_LENGTH)), Optional.empty(), original.arrayDimensions());
            case "varbinary" -> getJDBCTypeHandle(Types.VARBINARY, Optional.ofNullable(typeName), original.arrayDimensions());
            case "time" -> getJDBCTypeHandle(Types.TIME, Optional.ofNullable(typeName), Optional.empty(), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "time with time zone" -> getJDBCTypeHandle(Types.TIME_WITH_TIMEZONE, Optional.ofNullable(typeName), Optional.empty(), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "timestamp" -> getJDBCTypeHandle(Types.TIMESTAMP, Optional.ofNullable(typeName), Optional.empty(), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "timestamp with time zone" -> getJDBCTypeHandle(Types.TIMESTAMP_WITH_TIMEZONE, Optional.ofNullable(typeName), Optional.empty(), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "unknown" -> getJDBCTypeHandle(Types.NULL, Optional.ofNullable(typeName), original.arrayDimensions());
            case "json" -> getJDBCTypeHandle(Types.JAVA_OBJECT, Optional.ofNullable(typeName), original.arrayDimensions());
            case "date" -> getJDBCTypeHandle(Types.DATE, Optional.ofNullable(typeName), original.arrayDimensions());
            case "decimal" -> getJDBCTypeHandle(Types.DECIMAL, Optional.ofNullable(typeName), Optional.of(original.requiredColumnSize()), Optional.of(original.requiredDecimalDigits()), original.arrayDimensions());
            case "array" -> getJDBCTypeHandle(Types.ARRAY, Optional.of(elementTypeName), original.columnSize(), original.decimalDigits(), original.arrayDimensions());
            case "row" -> getJDBCTypeHandle(Types.STRUCT, Optional.of(elementTypeName), original.columnSize(), original.decimalDigits(), original.arrayDimensions());
            case "map" -> getJDBCTypeHandle(Types.JAVA_OBJECT, Optional.of(elementTypeName), original.columnSize(), original.decimalDigits(), original.arrayDimensions());
            default -> getJDBCTypeHandle(Types.JAVA_OBJECT, Optional.of(elementTypeName), original.columnSize(), original.decimalDigits(), original.arrayDimensions());
        };
    }

    private static JdbcTypeHandle getJDBCTypeHandle(int jdbcType, Optional<String> typeName)
    {
        return getJDBCTypeHandle(jdbcType, typeName, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static JdbcTypeHandle getJDBCTypeHandle(int jdbcType, Optional<String> typeName, Optional<Integer> arraySize)
    {
        return getJDBCTypeHandle(jdbcType, typeName, Optional.empty(), Optional.empty(), arraySize);
    }

    private static JdbcTypeHandle getJDBCTypeHandle(int jdbcType, Optional<String> typeName, Optional<Integer> columnSize, Optional<Integer> decimalDigits, Optional<Integer> arraySize)
    {
        return new JdbcTypeHandle(jdbcType, typeName, columnSize, decimalDigits, arraySize, Optional.empty());
    }

    private static SliceWriteFunction typedVarcharWriteFunction(String jdbcTypeName)
    {
        return new SliceWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return "json_parse(?)";
            }

            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                statement.setString(index, value.toStringUtf8());
            }
        };
    }

    private static TrinoConnection getConnection(Connection connection)
    {
        try {
            TrinoConnection remoteConnection = connection.unwrap(TrinoConnection.class);
            return remoteConnection;
        }
        catch (SQLException ex) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to get the TrinoConnection object from connection:" + ex.getMessage());
        }
    }

    /**
     * Replace all the tokens in an input using the algorithm provided for each

     * @param original original string
     * @param tokenPattern the pattern to match with
     * @param converter the conversion to apply
     * @return the substituted string
     */
    private static String replaceTokens(String original, Pattern tokenPattern, Function<Matcher, String> converter)
    {
        int lastIndex = 0;
        StringBuilder output = new StringBuilder();
        Matcher matcher = tokenPattern.matcher(original);
        while (matcher.find()) {
            output.append(original, lastIndex, matcher.start())
                    .append(converter.apply(matcher));

            lastIndex = matcher.end();
        }
        if (lastIndex < original.length()) {
            output.append(original, lastIndex, original.length());
        }
        return output.toString();
    }

    private Optional<String> mapTypeForCast(ConnectorSession connectorSession, Type type)
    {
        return Optional.ofNullable(type.getDisplayName());
    }

    public static LongWriteFunction trinoIntervalWriteMapping()
    {
        return LongWriteFunction.of(JAVA_OBJECT, (statement, index, intervalData) -> {
            statement.setObject(index, new TrinoIntervalDayTime(intervalData));
        });
    }

    public static WriteMapping trinoTimeWriteMapping(TimeType timeType)
    {
        int precision = timeType.getPrecision();
        return WriteMapping.longMapping(String.format("time(%s)", precision), timeWriteFunction(precision));
    }

    public static WriteMapping trinoTimeWithTimeZoneWriteMapping(TimeWithTimeZoneType timeWithTimeZoneType)
    {
        int precision = timeWithTimeZoneType.getPrecision();
        String dataType = String.format("time(%s) with time zone", precision);
        if (timeWithTimeZoneType.isShort()) {
            WriteMapping.longMapping(dataType, shortTimeWithTimeZoneWriteFunction(precision));
        }
        return WriteMapping.objectMapping(dataType, longTimeWithTimeZoneWriteFunction(precision));
    }

    private static LongWriteFunction shortTimeWithTimeZoneWriteFunction(int timePrecision)
    {
        return (statement, index, value) -> {
            String formatted = SqlTimeWithTimeZone.newInstance(timePrecision, unpackTimeNanos(value) * 1000L, unpackOffsetMinutes(value)).toString();
            statement.setObject(index, formatted, TIME_WITH_TIMEZONE);
        };
    }

    private static ObjectWriteFunction longTimeWithTimeZoneWriteFunction(int timePrecision)
    {
        return ObjectWriteFunction.of(LongTimeWithTimeZone.class, (statement, index, value) -> {
            String formatted = SqlTimeWithTimeZone.newInstance(timePrecision, value.getPicoseconds(), value.getOffsetMinutes()).toString();
            statement.setObject(index, formatted, TIME_WITH_TIMEZONE);
        });
    }

    public static WriteMapping trinoTimestampWriteMapping(TimestampType timestampType)
    {
        int precision = timestampType.getPrecision();
        String dataType = String.format("timestamp(%s)", precision);
        if (timestampType.isShort()) {
            return WriteMapping.longMapping(dataType, timestampWriteFunction(timestampType));
        }
        return WriteMapping.objectMapping(dataType, longTimestampWriteFunction(timestampType, precision));
    }

    public static WriteMapping trinoTimestampWithTimeZoneWriteMapping(TimestampWithTimeZoneType timestampWithTimeZoneType)
    {
        int precision = timestampWithTimeZoneType.getPrecision();
        String dataType = String.format("timestamp(%s) with time zone", precision);
        if (timestampWithTimeZoneType.isShort()) {
            WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction(precision));
        }
        return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction(precision));
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction(int timeStampPrecision)
    {
        return (statement, index, value) -> {
            String formatted = SqlTimestampWithTimeZone.newInstance(timeStampPrecision, unpackMillisUtc(value), 0, unpackZoneKey(value)).toString();
            statement.setObject(index, formatted, TIMESTAMP_WITH_TIMEZONE);
        };
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction(int timeStampPrecision)
    {
        return ObjectWriteFunction.of(LongTimestampWithTimeZone.class, (statement, index, value) -> {
            String formatted = SqlTimestampWithTimeZone.newInstance(timeStampPrecision, value.getEpochMillis(), value.getPicosOfMilli(), getTimeZoneKey(value.getTimeZoneKey())).toString();
            statement.setObject(index, formatted, TIMESTAMP_WITH_TIMEZONE);
        });
    }
}
