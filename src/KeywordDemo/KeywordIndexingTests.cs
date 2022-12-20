using KeywordDemo.Documents;
using TestInfrastructure;

namespace KeywordDemo
{
    public class KeywordIndexingTests : IClassFixture<IndexFixture>
    {
        private IndexFixture _fixture;

        public KeywordIndexingTests(IndexFixture fixture)
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
        [InlineData("Mouse", "Single word is indexed exactly as given")]
        [InlineData("Mouse pad", "Two words are indexed exactly as given")]
        [InlineData("This is a sentence! It contains some, really bad. Grammar;", "All grammar is indexed exactly as given")]
        public async Task KeywordMapping_IndexesASingleTokenForGivenString(string termText, string explanation)
        {
            var indexName = "keyword-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocument = new ProductDocument(1, termText);

                    await _fixture.IndexDocuments(uniqueIndexName, new[] { productDocument });

                    var result = await opensearchClient.TermVectorsAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Document(productDocument)
                       );

                    result.IsValid.Should().BeTrue();
                    var tokensAndFrequency = string.Join(", ", result.TermVectors.Values.SelectMany(value => value.Terms.Select(term => $"{term.Key}:{term.Value.TermFrequency}")));
                    var expectedTokenCsv = $"{termText}:1";
                    tokensAndFrequency.Should().BeEquivalentTo(expectedTokenCsv, explanation);
                }
            );
        }
    }
}
