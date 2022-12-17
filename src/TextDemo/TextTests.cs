using FluentAssertions;
using KeywordFilterType;
using TextDemo.Documents;

namespace TextDemo
{
    public class TextTests : IClassFixture<IndexFixture>
    {
        private IndexFixture _fixture;

        public TextTests(IndexFixture fixture)
        {
            _fixture = fixture;
        }

        [Theory]
        /// <summary>
        /// Additional reading about the standard analyzer can be found here: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-standard-analyzer.html
        /// </summary>
        [
            InlineData(
            "product", 
            new[] { "product" }, 
            "The standard analyzer does not produce additional tokens for single word strings")
        ]
        [
            InlineData(
            "great product", 
            new[] { "great", "product" }, 
            "Two tokens are produced by the standard analyzer as it provides grammar based tokenisation")
        ]
        [
            InlineData(
            "This is a really amazing product. You absolutely must buy it!", 
            new[] { "this", "is", "a", "really", "amazing", "product", "you", "absolutely", "must", "buy", "it" }, 
            "Many tokens are produced. Gramma is stripped so that only words are indexed. Case is normalised to lowercase")]
        public async Task TextMapping_UsesStandardAnalyzerByDefault(string text, string[] expectedTokens, string explanation)
        {
            var indexName = "test-index";
            await _fixture.PerformActionInTestIndex<ProductDocument>(
                indexName,
                mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Text(word => word.Name(name => name.Description))
                    ),
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocument = new ProductDocument(1, text);

                    await _fixture.IndexDocuments(uniqueIndexName, new[] { productDocument });

                    var result = await opensearchClient.TermVectorsAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Document(productDocument)
                       );

                    result.IsValid.Should().BeTrue();
                    result.TermVectors.Values.SelectMany(value => value.Terms.Select(term => term.Key)).Should().BeEquivalentTo(expectedTokens, explanation);
                }
            );
        }
    }
}