/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.MapExpression

class MapExpressionParserTest extends AstParsingTestBase {

  test("valid map with single element should parse") {
    "{key: 'value'}" should parseTo[MapExpression](mapOf("key" -> literalString("value")))
    "{key: 42}" should parseTo[MapExpression](mapOf("key" -> literalInt(42)))
    "{key: 0.42}" should parseTo[MapExpression](mapOf("key" -> literalFloat(0.42)))
    "{key: false}" should parseTo[MapExpression](mapOf("key" -> falseLiteral))
  }

  test("empty map should parse") {
    "{}" should parseTo[MapExpression](mapOf())
  }

  test("map with mixed element types should parse") {
    "{key1: 'value', key2: false, key3: 42}" should parseTo[MapExpression](
      mapOf("key1" -> literalString("value"), "key2" -> falseLiteral, "key3" -> literalInt(42))
    )
  }

  test("map with non-string key should not parse") {
    "{42: 'value'}" should notParse[MapExpression]
  }

  test("map without comma separation should not parse") {
    "{key1: 'value' key2: 42}" should notParse[MapExpression]
  }

  test("map with invalid start comma should not parse") {
    "{, key: 'value'}" should notParse[MapExpression]
  }
}