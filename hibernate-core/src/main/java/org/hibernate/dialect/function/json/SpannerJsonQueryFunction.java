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
 * Spanner-specific json_query function.
 * <p>
 * This override is needed because Spanner's native {@code json_query}
 * does not support the expected {@code WITH WRAPPER} and {@code WITHOUT WRAPPER} clauses
 * natively. It also lacks support for certain {@code ON ERROR} or {@code ON EMPTY}
 * behaviors matching SQL/JSON standards.
 * <p>
 * Therefore, this implementation strips out those unsupported clauses completely,
 * explicitly appending only the json document and the path.
 */
public class SpannerJsonQueryFunction extends JsonQueryFunction {

	public SpannerJsonQueryFunction(TypeConfiguration typeConfiguration) {
		super( typeConfiguration, true, false );
	}

	@Override
	protected void render(
			SqlAppender sqlAppender,
			JsonQueryArguments arguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> walker) {
		if ( arguments.errorBehavior() != null ) {
			throw new QueryException( "Spanner does not support ON ERROR clause for json_query" );
		}
		if ( arguments.emptyBehavior() != null ) {
			throw new QueryException( "Spanner does not support ON EMPTY clause for json_query" );
		}

		sqlAppender.appendSql( "json_query(" );
		arguments.jsonDocument().accept( walker );
		sqlAppender.appendSql( "," );
		arguments.jsonPath().accept( walker );
		sqlAppender.appendSql( ")" );
	}
}
