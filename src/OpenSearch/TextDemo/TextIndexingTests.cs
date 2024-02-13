using FluentAssertions;
using OpenSearch.Client;
using OpenSearchTestInfrastructure;
using OpenSearchTextDemo.Documents;

namespace OpenSearchKeywordDemo
{
    public class TextIndexingTests : IClassFixture<OpenSearchIndexFixture>
    {
        private OpenSearchIndexFixture _fixture;

        public TextIndexingTests(OpenSearchIndexFixture fixture)
        {
            _fixture = fixture;
        }

        /// <summary>
        /// This function is used to define a text mapping for the description of a product
        /// Opensearch documentation: https://opensearch.org/docs/latest/field-types/supported-field-types/text/
        /// ElasticSearch documentation is far richer in very similar detail: https://www.elastic.co/guide/en/elasticsearch/reference/current/text.html
        /// </summary>
        Func<TypeMappingDescriptor<ProductDocument>, ITypeMapping> mappingDescriptor = mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Text(word => word.Name(name => name.Description))
                    );

        /// <summary>
        /// <see cref="KeywordIndexingTests"/> to compare how keywords differ to text fields
        /// </summary>
        [Theory]
        [InlineData("Mouse", new[] { "mouse:1" }, "Single word is indexed exactly as given")]
        [InlineData("Mouse pad", new[] { "mouse:1", "pad:1" }, "Two words are stored as separate tokens")]
        [InlineData(
            "This is a sentence! It contains some, really bad. Grammar; sentence",
            new[] { "a:1", "bad:1", "contains:1", "grammar:1", "is:1", "it:1", "really:1", "sentence:2", "some:1", "this:1" },
            "Grammar is removed and whole words are stored as tokens, lowercase normalised"
            )]
        public async Task TextMapping_IndexesUsingStandardTokensiserForGivenString(string description, string[] expectedTokensAndFrequencies, string explanation)
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);

            var mappingRequest = new GetMappingRequest();
            var mappingResult = await _fixture.OpenSearchClient.Indices.GetMappingAsync(mappingRequest);

            var productDocument = new ProductDocument(1, description);

            await testIndex.IndexDocuments(new[] { productDocument });

            var result = await _fixture.OpenSearchClient.TermVectorsAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .Document(productDocument)
               );

            result.IsValid.Should().BeTrue();
            // Each token is parsed from the response, against the number of times it appeared in the given string
            var tokensAndFrequency = result.TermVectors.Values.SelectMany(value => value.Terms.Select(term => $"{term.Key}:{term.Value.TermFrequency}"));
            tokensAndFrequency.Should().BeEquivalentTo(expectedTokensAndFrequencies, options => options.WithStrictOrdering(), explanation);
        }
    }
}
