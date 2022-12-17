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
        /// Additional reading about text anaysis can be found here: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-overview.html
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
            var indexName = "text-index";
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

        [Theory]
        /// <summary>
        /// This test creates a new custom analyzer of type "standard" (the standard analyzer), which leverages its default tokenizers and token filters
        /// and applies custom stop words which will not be tokenised
        /// Read more about stop token filter here: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-stop-tokenfilter.html
        /// </summary>
        [
            InlineData(
            "This is a GREAT product!",
            new[] { "this", "is", "a" },
            "great:1, product:1",
            "The standard analyzer does not produce additional tokens for single word strings")
        ]
        public async Task TextMapping_CanBeConfiguredToUseStopWordsTokenFilterToOmitWordsAsTokens(string text, string[] stopWords, string expectedTokensCsv, string explanation)
        {
            var indexName = "text-index";
            var customAnalyzerName = "customAnalyzer";

            await _fixture.PerformActionInTestIndex<ProductDocument>(
                indexName,
                 settings => settings
                // We can configure our custom analyzer via settings on the index
                .Analysis(analysis => analysis
                    .Analyzers(analyzers => analyzers
                    // Set stopwords to our custom list
                    .Standard(customAnalyzerName, selector => selector.StopWords(stopWords)
                    ))
                ),
                mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Text(word => word
                            .Name(name => name.Description)
                            // Set our custom analyzer on this text mapping so that we apply the stop token filter during indexing
                            .Analyzer(customAnalyzerName)
                    )),
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