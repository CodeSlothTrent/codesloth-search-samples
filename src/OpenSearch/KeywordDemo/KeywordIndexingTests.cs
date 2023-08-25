using OpenSearchKeywordDemo.Documents;
using OpenSearchTestInfrastructure;

namespace OpenSearchKeywordDemo
{
    public class KeywordIndexingTests : IClassFixture<OpenSearchIndexFixture>
    {
        private OpenSearchIndexFixture _fixture;

        public KeywordIndexingTests(OpenSearchIndexFixture fixture)
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
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);
            var productDocument = new ProductDocument(1, termText);

            await testIndex.IndexDocuments(new[] { productDocument });

            var result = await _fixture.OpenSearchClient.TermVectorsAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .Document(productDocument)
               );

            result.IsValid.Should().BeTrue();
            var tokensAndFrequency = string.Join(", ", result.TermVectors.Values.SelectMany(value => value.Terms.Select(term => $"{term.Key}:{term.Value.TermFrequency}")));
            var expectedTokenCsv = $"{termText}:1";
            tokensAndFrequency.Should().BeEquivalentTo(expectedTokenCsv, explanation);
        }
    }
}
