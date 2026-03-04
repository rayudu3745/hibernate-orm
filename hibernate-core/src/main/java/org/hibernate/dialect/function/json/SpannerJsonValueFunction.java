/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.function.json;

import org.hibernate.QueryException;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner-specific json_value function.
 * <p>
 * This override is needed because Spanner's native {@code json_value} function
 * does not natively support the implicit {@code RETURNING type} cast clause within
 * the function arguments itself. It also lacks compliant support for {@code ON EMPTY}
 * and {@code ON ERROR} handlers.
 * <p>
 * Therefore, this implementation strips those invalid clauses, and dynamically surrounds
 * the native {@code json_value} call with a {@code CAST(... AS type)} instead to enforce
 * the application of the expected {@code RETURNING} type at the query level.
 */
public class SpannerJsonValueFunction extends JsonValueFunction {

	public SpannerJsonValueFunction(TypeConfiguration typeConfiguration) {
		super( typeConfiguration, true, false );
	}

	@Override
	protected void render(
			SqlAppender sqlAppender,
			JsonValueArguments arguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> walker) {
		if ( arguments.errorBehavior() != null ) {
			throw new QueryException( "Spanner does not support ON ERROR clause for json_value" );
		}
		if ( arguments.emptyBehavior() != null ) {
			throw new QueryException( "Spanner does not support ON EMPTY clause for json_value" );
		}

		final boolean needsCast = arguments.returningType() != null;
		if ( needsCast ) {
			sqlAppender.appendSql( "cast(" );
		}

		sqlAppender.appendSql( "json_value(" );
		arguments.jsonDocument().accept( walker );
		sqlAppender.appendSql( "," );
		arguments.jsonPath().accept( walker );
		sqlAppender.appendSql( ")" );

		if ( needsCast ) {
			sqlAppender.appendSql( " as " );
			arguments.returningType().accept( walker );
			sqlAppender.appendSql( ")" );
		}
	}
}
