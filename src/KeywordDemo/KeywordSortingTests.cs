using KeywordDemo.Documents;
using TestInfrastructure;

namespace KeywordDemo
{
    public class KeywordSortingTests : IClassFixture<IndexFixture>
    {
        private IndexFixture _fixture;

        public KeywordSortingTests(IndexFixture fixture)
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
            );
        }

        [Fact]
        /// <summary>
        /// Keyword fields do not require anything special to support sorting
        /// </summary>
        public async Task KeywordMapping_CanBeUsedAsASortedField_WithoutAnySpecialConsiderations()
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
            );
        }

        [Fact]
        public async Task KeywordMapping_ShouldNotBeUsedToSortNumericData()
        {
            var indexName = "keyword-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "5"),
    new ProductDocument(2, "2000"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(query => query.MatchAll())
                           .Explain()
                           .Sort(sort => sort
                            .Descending(fieldName => fieldName.Name)
                        )
                    );

                    // Our documents can be sorted alphabetically
                    result.IsValid.Should().BeTrue();
                    var formattedResults = string.Join(", ", result.Documents.Select(doc => doc.Name));
                    formattedResults.Should().BeEquivalentTo("2000, 5");
                }
            );
        }
    }
}
