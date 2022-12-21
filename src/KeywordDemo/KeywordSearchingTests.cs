using KeywordDemo.Documents;
using TestInfrastructure;

namespace KeywordDemo
{
    public class KeywordSearchingTests : IClassFixture<IndexFixture>
    {
        private IndexFixture _fixture;

        public KeywordSearchingTests(IndexFixture fixture)
        {
            _fixture = fixture;
        }

        /// <summary>
        /// This function is used to define a keyword mapping for the Name of a product
        /// Opensearch documentation: https://opensearch.org/docs/2.0/opensearch/supported-field-types/keyword/
        /// ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
        /// </summary>
        Func<TypeMappingDescriptor<ProductDocument>, ITypeMapping> mappingDescriptor = mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Keyword(word => word.Name(name => name.Name))
                    );

        [Theory]
        [InlineData("mouse", "Only the document with name mouse will match")]
        [InlineData("mouse pad", "Only the document with name mouse pad will match")]
        public async Task KeywordMapping_ExactlyMatchesWholeTermQuery(string termText, string explanation)
        {
            var indexName = "keyword-index";
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
        [InlineData("mouse", "Only the document with name mouse will match")]
        [InlineData("mouse pad", "Only the document with name mouse pad will match")]
        public async Task KeywordMapping_CanBeFilteredOnWithBooleanQuery(string termText, string explanation)
        {
            var indexName = "keyword-index";
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
                                .Bool(boolQuery => boolQuery
                                    .Filter(filter => filter
                                        .Term(term => term
                                        .Field(field => field.Name)
                                        .Value(termText)
                                        ))
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
        [InlineData("mouse", "Only the document with name mouse will match")]
        [InlineData("mouse pad", "Only the document with name mouse pad will match")]
        public async Task KeywordMapping_CanBeFilteredAndScoredOnWithConstantScoreQuery(string termText, string explanation)
        {
            var indexName = "keyword-index";
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
                                .ConstantScore(boolQuery => boolQuery
                                    .Filter(filter => filter
                                        .Term(term => term
                                        .Field(field => field.Name)
                                        .Value(termText)
                                        ))
                                    .Boost(3)
                                   )
                               )
                           .Explain()
                       );

                    result.IsValid.Should().BeTrue();
                    result.Documents.Should().ContainSingle(doc => string.Equals(doc.Name, termText), explanation);
                    result.Hits.Single().Score.Should().Be(3);
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
            var indexName = "keyword-index";
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
        [InlineData("Mouse pad", "Missing a space")]
        public async Task KeywordMapping_DoesNotMatchOnSlightlyMismatchedTerms(string termText, string explanation)
        {
            var indexName = "keyword-index";
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
    }
}
