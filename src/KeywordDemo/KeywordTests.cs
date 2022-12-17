﻿using KeywordDemo.Documents;

namespace KeywordFilterType
{
    public class KeywordTests : IClassFixture<IndexFixture>
    {
        private IndexFixture _fixture;

        public KeywordTests(IndexFixture fixture)
        {
            _fixture = fixture;
        }

        Func<TypeMappingDescriptor<ProductDocument>, ITypeMapping> mappingDescriptor = mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Number(number => number.Name(name => name.Id))
                        .Keyword(word => word.Name(name => name.Name))
                    );

        [Theory]
        [InlineData("mouse", "Only the document with name mouse will match")]
        [InlineData("mouse pad", "Only the document with name mouse pad will match")]
        public async Task KeywordMapping_ExactlyMatchesWholeTermQuery(string termText, string explanation)
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(queryContainer => queryContainer
                               .Term(term => term
                                   .Field(field => field.Name)
                                   .Value(termText)
                                   )
                               )
                           .Explain()
                       );

                    result.IsValid.Should().BeTrue();
                    result.Documents.Should().ContainSingle(doc => string.Equals(doc.Name, termText), explanation);
                }
            );
        }

        [Theory]
        [InlineData("mouse", new[] { "mouse" }, "Only the document with name mouse will match")]
        [InlineData("mouse pad", new[] { "mouse", "pad" },
            @"If the standard analyzer was run on this text it would produce two tokens: mouse, pad. 
            Neither individual token would exactly match the mouse pad document name resulting in no document being returned. 
            However, OepnSearch identifies that the mapping of the field is not Text and does not apply an analyzer at query time. 
            This default behaviour only applies for text field mappings.")]
        public async Task KeywordMapping_ExactlyMatchesKeywordQuery_BecauseNoQueryTimeAnalyzerIsUsedOnGivenText(string matchText, string[] expectedTokens, string explanation)
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(queryContainer => queryContainer
                               .Match(term => term
                                   .Field(field => field.Name)
                                   .Query(matchText)
                                   )
                               )
                           .Explain()
                       );

                    result.IsValid.Should().BeTrue();
                    result.Documents.Should().ContainSingle(doc => string.Equals(doc.Name, matchText), explanation);

                    // Let's confirm the tokens that WOULD have been generated if we used a match query on a TEXT field mapping
                    var analyzeResult = await opensearchClient.Indices.AnalyzeAsync(selector => selector
                        .Analyzer("standard")
                        .Index(uniqueIndexName)
                        .Text(matchText));

                    analyzeResult.Tokens.Select(token => token.Token).Should().BeEquivalentTo(expectedTokens);
                }
            );
        }

        [Theory]
        [InlineData("mous", "Missing a letter")]
        [InlineData("mousepad", "Missing a space")]
        public async Task KeywordMapping_DoesNotMatchOnSlightlyMismatchedTerms(string termText, string explanation)
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(queryContainer => queryContainer
                               .Match(term => term
                                   .Field(field => field.Name)
                                   .Query(termText)
                                   )
                               )
                       );

                    result.IsValid.Should().BeTrue();
                    result.Documents.Should().BeEmpty(explanation);
                }
            );
        }

        [Fact]
        public async Task KeywordMapping_CanBeUsedToCreateAScriptedField()
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    const string categoryFieldName = nameof(ScriptedProductDocument.Category);

                    var result = await opensearchClient.SearchAsync<ScriptedProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .ScriptFields(scriptFields => scriptFields
                            .ScriptField(
                               categoryFieldName,
                               selector => selector.Source($"doc['{nameof(ProductDocument.Name).ToLowerInvariant()}'].value == 'mouse' ? 'computer accessory' : 'mouse accessory'"))
                            )
                           .Source(true)
                       );

                    result.IsValid.Should().BeTrue();

                    // Cannot get ValueOf<TDoc, TFieldType>() working at time of writing - it always returns null.
                    foreach (var hit in result.Hits)
                    {
                        hit.Fields.ValueOf<ScriptedProductDocument, string>(doc => doc.Category).Should().BeNull();
                    }

                    // Using Value<TFieldType> with string lookup instead
                    var formattedResults = string.Join(", ", result.Hits.Select(hit => $"{hit.Source.Name}:{hit.Fields.Value<string>(categoryFieldName)}"));
                    formattedResults.Should().BeEquivalentTo("mouse:computer accessory, mouse pad:mouse accessory");
                }
            );
        }

        [Fact]
        public async Task KeywordMapping_CanBeUsedToScriptASortedField()
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(query => query.MatchAll())
                           .Sort(sort => sort
                            .Script(sortScript => sortScript
                                .Ascending()
                                .Type("number")
                                .Script(s => s.Source($"doc['{nameof(ProductDocument.Name).ToLowerInvariant()}'].value == 'mouse pad' ? 0 : 1")
                                )
                            )
                        )
                    );

                    // Our scripted sort will return the mousepad at the top of the results
                    result.IsValid.Should().BeTrue();
                    var formattedResults = string.Join(", ", result.Documents.Select(doc => doc.Name));
                    formattedResults.Should().BeEquivalentTo("mouse pad, mouse");
                }
            );
        }

        [Fact]
        /// <summary>
        /// Keyword fields do not require the FieldData mapping for sorting
        /// </summary>
        public async Task KeywordMapping_CanBeUsedAsASortedField_WithoutSpecifyingFieldDataInMapping()
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(query => query.MatchAll())
                           .Sort(sort => sort
                            .Descending(fieldName => fieldName.Name)
                        )
                    );

                    // Our documents can be sorted alphabetically
                    result.IsValid.Should().BeTrue();
                    var formattedResults = string.Join(", ", result.Documents.Select(doc => doc.Name));
                    formattedResults.Should().BeEquivalentTo("mouse pad, mouse");
                }
            );
        }

        [Fact]
        /// <summary>
        /// Keyword fields do not require the FieldData mapping for aggregations
        /// </summary>
        public async Task KeywordMapping_CanBeUsedAsAnAggregationField_WithoutSpecifyingFieldDataInMapping()
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(3, "mouse pad"),
    new ProductDocument(4, "mouse"),
    new ProductDocument(5, "mouse"),
    new ProductDocument(6, "mouse pad"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    const string productCounts = "productCounts";

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(query => query.MatchAll())
                           // We do not want any documents returned; just the aggregations
                           .Size(0)
                           .Aggregations(aggregations => aggregations
                            .Terms(productCounts, termSelector => termSelector.Field(field => field.Name))
                        )
                    );
                        
                    // Our documents can be sorted alphabetically
                    result.IsValid.Should().BeTrue();
                    var formattedResults = string.Join(", ", result.Aggregations
                        .Terms(productCounts).Buckets
                        .Select(bucket => $"{bucket.Key}:{bucket.DocCount}")
                    );

                    formattedResults.Should().BeEquivalentTo("mouse:3, mouse pad:2");
                }
            );
        }
    }
}
