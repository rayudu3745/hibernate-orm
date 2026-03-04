/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.aggregate;

import org.hibernate.internal.util.StringHelper;
import org.hibernate.mapping.Column;
import org.hibernate.metamodel.mapping.JdbcMapping;
import org.hibernate.metamodel.mapping.SelectableMapping;
import org.hibernate.metamodel.mapping.SelectablePath;
import org.hibernate.metamodel.mapping.SqlTypedMapping;
import org.hibernate.sql.ast.SqlAstNodeRenderingMode;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.type.spi.TypeConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.hibernate.type.SqlTypes.BINARY;
import static org.hibernate.type.SqlTypes.BLOB;
import static org.hibernate.type.SqlTypes.BOOLEAN;
import static org.hibernate.type.SqlTypes.JSON;
import static org.hibernate.type.SqlTypes.JSON_ARRAY;
import static org.hibernate.type.SqlTypes.LONG32VARBINARY;
import static org.hibernate.type.SqlTypes.VARBINARY;

public class SpannerAggregateSupport extends AggregateSupportImpl {

	public static final AggregateSupport INSTANCE = new SpannerAggregateSupport();

	private SpannerAggregateSupport() {
	}

	@Override
	public String aggregateComponentCustomReadExpression(
			String template,
			String placeholder,
			String aggregateParentReadExpression,
			String columnExpression,
			int aggregateColumnTypeCode,
			SqlTypedMapping column,
			TypeConfiguration typeConfiguration) {
		if ( aggregateColumnTypeCode != JSON_ARRAY && aggregateColumnTypeCode != JSON ) {
			throw new IllegalArgumentException( "Unsupported aggregate SQL type: " + aggregateColumnTypeCode );
		}

		return switch ( column.getJdbcMapping().getJdbcType().getDefaultSqlTypeCode() ) {
			case JSON, JSON_ARRAY -> template.replace(
					placeholder,
					queryExpression( aggregateParentReadExpression, columnExpression )
			);
			case BINARY, VARBINARY, LONG32VARBINARY ->
				// We encode binary data as hex, so we have to decode here
					template.replace(
							placeholder,
							"from_hex(cast(json_value(" + aggregateParentReadExpression + ",'$." + columnExpression + "') as string))"
					);

			case BOOLEAN -> template.replace(
					placeholder,
					"cast(json_value(" + aggregateParentReadExpression + ",'$." + columnExpression + "') as bool)"
			);
			default -> template.replace(
					placeholder,
					valueExpression( aggregateParentReadExpression, columnExpression, columnCastType( column ) )
			);
		};
	}

	private String columnCastType(SqlTypedMapping column) {
		final int sqlTypeCode = column.getJdbcMapping().getJdbcType().getDefaultSqlTypeCode();
		switch ( sqlTypeCode ) {
			case org.hibernate.type.SqlTypes.DATE,
				org.hibernate.type.SqlTypes.TIME,
				org.hibernate.type.SqlTypes.TIME_WITH_TIMEZONE,
				org.hibernate.type.SqlTypes.TIMESTAMP,
				org.hibernate.type.SqlTypes.TIMESTAMP_UTC,
				org.hibernate.type.SqlTypes.TIMESTAMP_WITH_TIMEZONE,
				org.hibernate.type.SqlTypes.UUID -> {
				return "string";
			}
		}
		String definition = column.getColumnDefinition();
		if ( definition == null ) {
			return "string";
		}
		int parenIndex = definition.indexOf('(');
		if (parenIndex != -1) {
			return definition.substring(0, parenIndex);
		}
		return definition;
	}

	private String valueExpression(String aggregateParentReadExpression, String columnExpression, String columnType) {
		return "cast(json_value(" + aggregateParentReadExpression + ",'$." + columnExpression + "') as " + columnType + ')';
	}

	private String queryExpression(String aggregateParentReadExpression, String columnExpression) {
		return "json_query(" + aggregateParentReadExpression + ",'$." + columnExpression + "')";
	}

	private String jsonCustomWriteExpression(String customWriteExpression, JdbcMapping jdbcMapping) {
		final int sqlTypeCode = jdbcMapping.getJdbcType().getDefaultSqlTypeCode();
		return switch ( sqlTypeCode ) {
			case BINARY, VARBINARY, LONG32VARBINARY, BLOB ->
				// We encode binary data as hex
					"to_hex(" + customWriteExpression + ")";

			case org.hibernate.type.SqlTypes.TIMESTAMP ->
				// GoogleSQL implicitly casts timestamps to JSON strings with a 'Z' (UTC format).
				// We strip it for LocalDateTime compatibility via format_timestamp.
					"format_timestamp('%Y-%m-%dT%H:%M:%E*S', " + customWriteExpression + ")";
			case org.hibernate.type.SqlTypes.TIMESTAMP_UTC, org.hibernate.type.SqlTypes.TIMESTAMP_WITH_TIMEZONE ->
					"format_timestamp('%Y-%m-%dT%H:%M:%E*SZ', " + customWriteExpression + ")";
			case org.hibernate.type.SqlTypes.TIME ->
				// Extract the time part.
					"format_timestamp('%H:%M:%E*S', " + customWriteExpression + ")";
			case org.hibernate.type.SqlTypes.DATE -> "cast(" + customWriteExpression + " as string)";
			default -> customWriteExpression;
		};
	}

	@Override
	public String aggregateComponentAssignmentExpression(
			String aggregateParentAssignmentExpression,
			String columnExpression,
			int aggregateColumnTypeCode,
			Column column) {
		return switch ( aggregateColumnTypeCode ) {
			case JSON, JSON_ARRAY ->
				// For JSON we always have to replace the whole object
					aggregateParentAssignmentExpression;
			default -> throw new IllegalArgumentException( "Unsupported aggregate SQL type: " + aggregateColumnTypeCode );
		};
	}

	@Override
	public boolean requiresAggregateCustomWriteExpressionRenderer(int aggregateSqlTypeCode) {
		return switch ( aggregateSqlTypeCode ) {
			case JSON -> true;
			default -> false;
		};
	}

	@Override
	public WriteExpressionRenderer aggregateCustomWriteExpressionRenderer(
			SelectableMapping aggregateColumn,
			SelectableMapping[] columnsToUpdate,
			TypeConfiguration typeConfiguration) {
		final int aggregateSqlTypeCode = aggregateColumn.getJdbcMapping().getJdbcType().getDefaultSqlTypeCode();
		return switch ( aggregateSqlTypeCode ) {
			case JSON -> jsonAggregateColumnWriter( aggregateColumn, columnsToUpdate );
			default -> throw new IllegalArgumentException( "Unsupported aggregate SQL type: " + aggregateSqlTypeCode );
		};
	}

	private WriteExpressionRenderer jsonAggregateColumnWriter(
			SelectableMapping aggregateColumn,
			SelectableMapping[] columns) {
		return new RootJsonWriteExpression( aggregateColumn, columns );
	}

	interface JsonWriteExpression {
		void append(
				SqlAppender sb,
				String path,
				SqlAstTranslator<?> translator,
				AggregateColumnWriteExpression expression);
	}
	private class AggregateJsonWriteExpression implements JsonWriteExpression {
		private final LinkedHashMap<String, JsonWriteExpression> subExpressions = new LinkedHashMap<>();

		protected void initializeSubExpressions(SelectableMapping[] columns) {
			for ( SelectableMapping column : columns ) {
				final SelectablePath selectablePath = column.getSelectablePath();
				final SelectablePath[] parts = selectablePath.getParts();
				AggregateJsonWriteExpression currentAggregate = this;
				for ( int i = 1; i < parts.length - 1; i++ ) {
					currentAggregate = (AggregateJsonWriteExpression) currentAggregate.subExpressions.computeIfAbsent(
							parts[i].getSelectableName(),
							k -> new AggregateJsonWriteExpression()
					);
				}
				final String customWriteExpression = column.getWriteExpression();
				currentAggregate.subExpressions.put(
						parts[parts.length - 1].getSelectableName(),
						new BasicJsonWriteExpression(
								column,
								jsonCustomWriteExpression( customWriteExpression, column.getJdbcMapping() )
						)
				);
			}
		}

		@Override
		public void append(
				SqlAppender sb,
				String path,
				SqlAstTranslator<?> translator,
				AggregateColumnWriteExpression expression) {
			for ( Map.Entry<String, JsonWriteExpression> entry : subExpressions.entrySet() ) {
				final String column = entry.getKey();
				final JsonWriteExpression value = entry.getValue();
				final String subPath = queryExpression( path, column );
				sb.append( ',' );
				if ( value instanceof AggregateJsonWriteExpression ) {
					sb.append( "'$." );
					sb.append( column );
					sb.append( "',json_set(coalesce(" );
					sb.append( subPath );
					sb.append( ",json_object())" );
					value.append( sb, subPath, translator, expression );
					sb.append( ')' );
				}
				else {
					value.append( sb, subPath, translator, expression );
				}
			}
		}
	}

	private class RootJsonWriteExpression extends AggregateJsonWriteExpression
			implements WriteExpressionRenderer {
		private final boolean nullable;
		private final String path;

		RootJsonWriteExpression(SelectableMapping aggregateColumn, SelectableMapping[] columns) {
			this.nullable = aggregateColumn.isNullable();
			this.path = aggregateColumn.getSelectionExpression();
			initializeSubExpressions( columns );
		}

		@Override
		public void render(
				SqlAppender sqlAppender,
				SqlAstTranslator<?> translator,
				AggregateColumnWriteExpression aggregateColumnWriteExpression,
				String qualifier) {
			final String basePath;
			if ( qualifier == null || qualifier.isBlank() ) {
				basePath = path;
			}
			else {
				basePath = qualifier + "." + path;
			}
			sqlAppender.appendSql( "json_set(" );
			if ( nullable ) {
				sqlAppender.append( "coalesce(" );
				sqlAppender.append( basePath );
				sqlAppender.append( ",json_object())" );
			}
			else {
				sqlAppender.append( basePath );
			}
			append( sqlAppender, basePath, translator, aggregateColumnWriteExpression );
			sqlAppender.append( ')' );
		}
	}
	private static class BasicJsonWriteExpression implements JsonWriteExpression {

		private final SelectableMapping selectableMapping;
		private final String customWriteExpressionStart;
		private final String customWriteExpressionEnd;

		BasicJsonWriteExpression(SelectableMapping selectableMapping, String customWriteExpression) {
			this.selectableMapping = selectableMapping;
			if ( customWriteExpression.equals( "?" ) ) {
				this.customWriteExpressionStart = "";
				this.customWriteExpressionEnd = "";
			}
			else {
				final String[] parts = StringHelper.split( "?", customWriteExpression );
				assert parts.length == 2;
				this.customWriteExpressionStart = parts[0];
				this.customWriteExpressionEnd = parts[1];
			}
		}

		@Override
		public void append(
				SqlAppender sb,
				String path,
				SqlAstTranslator<?> translator,
				AggregateColumnWriteExpression expression) {
			sb.append( "'$." );
			sb.append( selectableMapping.getSelectableName() );
			sb.append( "'," );
			sb.append( customWriteExpressionStart );
			// We use DEFAULT to avoid NO_UNTYPED appending parameterized lengths to typed CASTs,
			// as GoogleSQL restricts DDL parameterized sizing inside SQL CAST functions.
			// Spanner's JDBC driver correctly infers binding types.
			translator.render( expression.getValueExpression( selectableMapping ), SqlAstNodeRenderingMode.DEFAULT );
			sb.append( customWriteExpressionEnd );
		}
	}

}
