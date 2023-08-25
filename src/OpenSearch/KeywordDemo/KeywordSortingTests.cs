using OpenSearchKeywordDemo.Documents;
using OpenSearchTestInfrastructure;

namespace OpenSearchKeywordDemo
{
    public class KeywordSortingTests : IClassFixture<OpenSearchIndexFixture>
    {
        private OpenSearchIndexFixture _fixture;

        public KeywordSortingTests(OpenSearchIndexFixture fixture)
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

        [Fact]
        public async Task KeywordMapping_CanBeUsedToScriptASortedField()
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);
            var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};

            await testIndex.IndexDocuments(productDocuments);

            var result = await _fixture.OpenSearchClient.SearchAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .Query(query => query.MatchAll())
                   .Explain()
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

        [Fact]
        /// <summary>
        /// Keyword fields do not require anything special to support sorting
        /// </summary>
        public async Task KeywordMapping_CanBeUsedAsASortedField_WithoutAnySpecialConsiderations()
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);
            var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};
            await testIndex.IndexDocuments(productDocuments);

            var result = await _fixture.OpenSearchClient.SearchAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .Query(query => query.MatchAll())
                   .Explain()
                   .Sort(sort => sort
                    .Descending(fieldName => fieldName.Name)
                )
            );

            // Our documents can be sorted alphabetically
            result.IsValid.Should().BeTrue();
            var formattedResults = string.Join(", ", result.Documents.Select(doc => doc.Name));
            formattedResults.Should().BeEquivalentTo("mouse pad, mouse");
        }

        [Fact]
        public async Task KeywordMapping_ShouldNotBeUsedToSortNumericData()
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);
            var productDocuments = new[] {
    new ProductDocument(1, "5"),
    new ProductDocument(2, "2000"),
};
            await testIndex.IndexDocuments(productDocuments);

            var result = await _fixture.OpenSearchClient.SearchAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .Query(query => query.MatchAll())
                   .Explain()
                   .Sort(sort => sort
                    .Descending(fieldName => fieldName.Name)
                )
            );

            // Our documents can be sorted alphabetically
            result.IsValid.Should().BeTrue();
            var formattedResults = string.Join(", ", result.Documents.Select(doc => doc.Name));
            formattedResults.Should().BeEquivalentTo("5, 2000");
        }
    }
}
