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
                        .Keyword(word => word.Name(name => name.Name))
                        .Keyword(word => word.Name(name => name.Category)
                        )
                    );

        [Theory]
        [InlineData("mouse", "Only the document name mouse will match")]
        [InlineData("mouse pad", "Only the document name mouse pad will match")]
        public async Task KeywordMapping_ExactlyMatchesWholeTerm(string termText, string explanation)
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex<ProductDocument>(
                indexName,
                mappingDescriptor,
                async (opensearchClient) =>
                {
                    // Index some documents to test against
                    var productDocuments = new[] {
    new ProductDocument("mouse", "computing accessory"),
    new ProductDocument("mouse pad", "computing accessory"),
};

                    await _fixture.IndexDocuments(indexName, productDocuments);

                    var matchSearchResult = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(indexName)
                           .Query(queryContainer => queryContainer
                               .Match(term => term
                                   .Field(field => field.Name)
                                   .Query(termText)
                                   )
                               )
                       );

                    matchSearchResult.Documents.Should().ContainSingle(doc => string.Equals(doc.Name, termText), explanation);
                }
            );
        }

        [Theory]
        [InlineData("mous", "Missing a letter")]
        [InlineData("mousepad", "Missing a space")]
        public async Task KeywordMapping_DoesNotMatchMismatchedTerms(string termText, string explanation)
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex<ProductDocument>(
                indexName,
                mappingDescriptor,
                async (opensearchClient) =>
                {
                    // Index some documents to test against
                    var productDocuments = new[] {
    new ProductDocument("mouse", "computing accessory"),
    new ProductDocument("mouse pad", "computing accessory"),
};

                    await _fixture.IndexDocuments(indexName, productDocuments);

                    var matchSearchResult = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(indexName)
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
