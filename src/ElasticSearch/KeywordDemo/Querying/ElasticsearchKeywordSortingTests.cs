using ElasticSearchKeywordDemo.Documents;
using ElasticSearchTestInfrastructure;
using Nest;

namespace ElasticsearchKeywordDemo.Querying
{
    public class ElasticsearchKeywordSortingTests : IClassFixture<ElasticsearchIndexFixture>
    {
        private ElasticsearchIndexFixture _fixture;

        public ElasticsearchKeywordSortingTests(ElasticsearchIndexFixture fixture)
        {
            _fixture = fixture;
        }

        /// <summary>
        /// This function is used to define a keyword mapping for the Name of a product
        /// Opensearch documentation: https://opensearch.org/docs/2.0/opensearch/supported-field-types/keyword/
        /// ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/keyword.html
        /// </summary>
        Func<TypeMappingDescriptor<ElasticsearchProductDocument>, ITypeMapping> mappingDescriptor = mapping => mapping
                    .Properties<ElasticsearchProductDocument>(propertyDescriptor => propertyDescriptor
                        .Keyword(word => word.Name(name => name.Name))
                    );

        [Fact]
        public async Task KeywordMapping_CanBeUsedToScriptASortedField()
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);

            var productDocuments = new[]
            {
                new ElasticsearchProductDocument(1, "mouse"),
                new ElasticsearchProductDocument(2, "mouse pad"),
            };

            await testIndex.IndexDocuments(productDocuments);

            var result = await _fixture.ElasticClient.SearchAsync<ElasticsearchProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .Query(query => query.MatchAll())
                   .Explain()
                   .Sort(sort => sort
                    .Script(sortScript => sortScript
                        .Ascending()
                        .Type("number")
                        .Script(s => s.Source($"doc['{nameof(ElasticsearchProductDocument.Name).ToLowerInvariant()}'].value == 'mouse pad' ? 0 : 1")
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

            var productDocuments = new[]
            {
                new ElasticsearchProductDocument(1, "mouse"),
                new ElasticsearchProductDocument(2, "mouse pad"),
            };

            await testIndex.IndexDocuments(productDocuments);

            var result = await _fixture.ElasticClient.SearchAsync<ElasticsearchProductDocument>(selector => selector
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

            var productDocuments = new[]
            {
                new ElasticsearchProductDocument(1, "5"),
                new ElasticsearchProductDocument(2, "2000"),
            };

            await testIndex.IndexDocuments(productDocuments);

            var result = await _fixture.ElasticClient.SearchAsync<ElasticsearchProductDocument>(selector => selector
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
