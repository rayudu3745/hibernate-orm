/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.dialect.function.json;

import java.util.List;

import org.hibernate.QueryException;
import org.hibernate.metamodel.model.domain.ReturnableType;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.tree.SqlAstNode;
import org.hibernate.sql.ast.tree.expression.JsonNullBehavior;
import org.hibernate.type.spi.TypeConfiguration;

/**
 * Spanner-specific json_object function.
 * <p>
 * This override is needed because Spanner's native {@code json_object} function
 * does not support the expected colon syntax ({@code KEY : VALUE}) used by default
 * nor does it support modifiers like {@code NULL ON NULL}.
 * Therefore, this implementation iterates through the pairs, omitting unsupported SQL/JSON clauses.
 */
public class SpannerJsonObjectFunction extends JsonObjectFunction {

	public SpannerJsonObjectFunction(TypeConfiguration typeConfiguration) {
		super( typeConfiguration, false );
	}

	@Override
	public void render(
			SqlAppender sqlAppender,
			List<? extends SqlAstNode> sqlAstArguments,
			ReturnableType<?> returnType,
			SqlAstTranslator<?> walker) {
		sqlAppender.appendSql( "json_object(" );
		if ( !sqlAstArguments.isEmpty() ) {
			final SqlAstNode lastArgument = sqlAstArguments.get( sqlAstArguments.size() - 1 );
			final int argumentsCount;
			if ( lastArgument instanceof JsonNullBehavior jsonNullBehavior ) {
				if ( jsonNullBehavior == JsonNullBehavior.NULL ) {
					throw new QueryException( "Spanner does not support NULL ON NULL behavior for json_object" );
				}
				argumentsCount = ( sqlAstArguments.size() - 1 & 1 ) == 1 ? sqlAstArguments.size() - 2 : sqlAstArguments.size() - 1;
			}
			else {
				argumentsCount = ( sqlAstArguments.size() & 1 ) == 1 ? sqlAstArguments.size() - 1 : sqlAstArguments.size();
			}
			for ( int i = 0; i < argumentsCount; i++ ) {
				if ( i > 0 ) {
					sqlAppender.appendSql( "," );
				}
				sqlAstArguments.get( i ).accept( walker );
			}
		}
		sqlAppender.appendSql( ")" );
	}
}
