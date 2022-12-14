using FluentAssertions;
using KeywordFilterType.Documents;
using OpenSearch.Client;

namespace KeywordFilterType
{
    public class KeywordFilterTests : IClassFixture<IndexFixture>
    {
        private IndexFixture _fixture;

        public KeywordFilterTests(IndexFixture fixture)
        {
            _fixture = fixture;
        }

        Func<TypeMappingDescriptor<ProductDocument>, ITypeMapping> mappingDescriptor = mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Number(number => number.Name(name => name.Id))
                        .Keyword(word => word.Name(name => name.Name))
                        .Keyword(word => word.Name(name => name.Category))
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
    new ProductDocument(1, "mouse", "computing accessory"),
    new ProductDocument(2, "mouse pad", "computing accessory"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    var matchSearchResult = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(queryContainer => queryContainer
                               .Term(term => term
                                   .Field(field => field.Name)
                                   .Value(termText)
                                   )
                               )
                           .Explain()
                       );

                    matchSearchResult.Documents.Should().ContainSingle(doc => string.Equals(doc.Name, termText), explanation);
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
        public async Task KeywordMapping_ExactlyMatchesKeywordQuery_BecauseNoAnalyzerIsUsedOnGivenText(string matchText, string[] expectedTokens, string explanation)
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse", "computing accessory"),
    new ProductDocument(2, "mouse pad", "computing accessory"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    var matchSearchResult = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(queryContainer => queryContainer
                               .Match(term => term
                                   .Field(field => field.Name)
                                   .Query(matchText)
                                   )
                               )
                           .Explain()
                       );

                    matchSearchResult.Documents.Should().ContainSingle(doc => string.Equals(doc.Name, matchText), explanation);

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
        public async Task KeywordMapping_DoesNotMatchMismatchedTerms(string termText, string explanation)
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse", "computing accessory"),
    new ProductDocument(2, "mouse pad", "computing accessory"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    var matchSearchResult = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(queryContainer => queryContainer
                               .Match(term => term
                                   .Field(field => field.Name)
                                   .Query(termText)
                                   )
                               )
                       );

                    matchSearchResult.Documents.Should().BeEmpty(explanation);
                }
            );
        }
    }
}
