/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.orm.test.query.sqm.exec;

import org.hibernate.dialect.SpannerDialect;
import org.hibernate.testing.orm.domain.StandardDomainModel;
import org.hibernate.testing.orm.junit.DomainModel;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.hibernate.testing.orm.junit.SkipForDialect;
import org.junit.jupiter.api.Test;

@DomainModel( standardModels = StandardDomainModel.GAMBIT )
@SessionFactory
public class LiteralTests {

	@Test
	public void testTimestampLiteral(SessionFactoryScope scope) {
		final String queryString = "from EntityOfBasics e1 where e1.theTimestamp = {ts '2018-01-01T12:30:00'}";
		scope.inTransaction(
				session -> session.createQuery( queryString ).list()
		);
	}

	@Test
	public void testDateLiteral(SessionFactoryScope scope) {
		scope.inTransaction(
				session -> {
					final String queryString = "from EntityOfBasics e1 where e1.theDate = {d '2018-01-01'}";
					session.createQuery( queryString ).list();
				}
		);
	}

	@Test
	@SkipForDialect(dialectClass = SpannerDialect.class, reason = "Spanner doesn't support time literals")
	public void testTimeLiteral(SessionFactoryScope scope) {
		scope.inTransaction(
				session -> {
					final String queryString = "from EntityOfBasics e1 where e1.theTime = {t '12:30:00'}";
					session.createQuery( queryString ).list();
				}
		);
	}
}
