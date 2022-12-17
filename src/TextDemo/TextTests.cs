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
            "product:1", 
            "The standard analyzer does not produce additional tokens for single word strings")
        ]
        [
            InlineData(
            "great product", 
            "great:1, product:1", 
            "Two individual tokens are produced by the standard analyzer as it provides grammar based tokenisation")
        ]
        [
            InlineData(
            "This is a really amazing product. It is great, you absolutely must buy it!",
            "a:1, absolutely:1, amazing:1, buy:1, great:1, is:2, it:2, must:1, product:1, really:1, this:1, you:1",
            "Many tokens are produced. Gramma is stripped so that only words are indexed. Case is normalised to lowercase. Recurring words are counted against the same token.")]
        public async Task TextMapping_UsesStandardAnalyzerByDefault(string text, string expectedTokensCsv, string explanation)
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

                    // TermVectors will return us the indexed tokens for our field
                    var result = await opensearchClient.TermVectorsAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Document(productDocument)
                       );

                    result.IsValid.Should().BeTrue();
                    var tokensAndFrequency = string.Join(", ", result.TermVectors.Values.SelectMany(value => value.Terms.Select(term => $"{term.Key}:{term.Value.TermFrequency}")));
                    tokensAndFrequency.Should().BeEquivalentTo(expectedTokensCsv, explanation);
                }
            );
        }
    }
}