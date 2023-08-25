using FluentAssertions;
using OpenSearchTestInfrastructure;
using OpenSearchTextDemo.Documents;

namespace OpenSearchTextDemo
{
    public class TextTests : IClassFixture<OpenSearchIndexFixture>
    {
        private OpenSearchIndexFixture _fixture;

        public TextTests(OpenSearchIndexFixture fixture)
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
            await using var testIndex = await _fixture.CreateTestIndex<ProductDocument>(mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Text(word => word.Name(name => name.Description))
                    ));
            var productDocument = new ProductDocument(1, text);

            await testIndex.IndexDocuments(new[] { productDocument });

            // TermVectors will return us the indexed tokens for our field
            var result = await _fixture.OpenSearchClient.TermVectorsAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .Document(productDocument)
               );

            result.IsValid.Should().BeTrue();
            var tokensAndFrequency = string.Join(", ", result.TermVectors.Values.SelectMany(value => value.Terms.Select(term => $"{term.Key}:{term.Value.TermFrequency}")));
            tokensAndFrequency.Should().BeEquivalentTo(expectedTokensCsv, explanation);
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
            "Our stopwords leave two tokens left for creation")
        ]
        [
            InlineData(
            "This is a GREAT product!",
            new[] { "_english_" },
            "great:1, product:1",
            "The english stopword defaults are defined in the link above and also include those which we specified manually in the prior test case")
        ]
        public async Task TextMapping_StandardAnalyzer_CanBeConfiguredToUseStopWordsTokenFilterToOmitWordsAsTokens(string text, string[] stopWords, string expectedTokensCsv, string explanation)
        {
            var customAnalyzerName = "customAnalyzer";
            await using var testIndex = await _fixture.CreateTestIndex<ProductDocument>(
                  mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Text(word => word
                            .Name(name => name.Description)
                            // Set our custom analyzer on this text mapping so that we apply the stop token filter during indexing
                            .Analyzer(customAnalyzerName)
                    )),
                      settings => settings
                // We can configure our custom analyzer via settings on the index
                .Analysis(analysis => analysis
                    .Analyzers(analyzers => analyzers
                    // Set stopwords to our custom list
                    .Standard(customAnalyzerName, selector => selector.StopWords(stopWords)
                    ))
                )
                    );
            var productDocument = new ProductDocument(1, text);

            await testIndex.IndexDocuments(new[] { productDocument });

            // TermVectors will return us the indexed tokens for our field
            var result = await _fixture.OpenSearchClient.TermVectorsAsync<ProductDocument>(selector => selector
                    .Index(testIndex.Name)
                    .Document(productDocument)
                );

            result.IsValid.Should().BeTrue();
            var tokensAndFrequency = string.Join(", ", result.TermVectors.Values.SelectMany(value => value.Terms.Select(term => $"{term.Key}:{term.Value.TermFrequency}")));
            tokensAndFrequency.Should().BeEquivalentTo(expectedTokensCsv, explanation);
        }

        [Theory]
        /// <summary>
        /// This test creates a new custom analyzer of type "standard" (the standard analyzer), which leverages its default tokenizers and token filters
        /// and applies custom maximum token length
        /// </summary>
        [
            InlineData(
            "GREAT product!",
            3,
            "at:1, duc:1, gre:1, pro:1, t:1",
            "The words are decomposed into smaller tokens: great = gre + at, product = pro + duc + t")
        ]
        [
            InlineData(
            "GREAT product!",
            5,
            "ct:1, great:1, produ:1",
            "The words are decomposed into smaller tokens: great = great, product = produ + ct")
        ]
        public async Task TextMapping_StandardAnalyzer_CanBeConfiguredWithMaximumTokenLength(string text, int maxTokenLength, string expectedTokensCsv, string explanation)
        {
            var customAnalyzerName = "customAnalyzer";

            await using var testIndex = await _fixture.CreateTestIndex<ProductDocument>(
                mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Text(word => word
                            .Name(name => name.Description)
                            // Set our custom analyzer on this text mapping so that we apply the stop token filter during indexing
                            .Analyzer(customAnalyzerName)
                    )),
                     settings => settings
                // We can configure our custom analyzer via settings on the index
                .Analysis(analysis => analysis
                    .Analyzers(analyzers => analyzers
                    // Set stopwords to our custom list
                    .Standard(customAnalyzerName, selector => selector.MaxTokenLength(maxTokenLength)
                    ))
                )
                );

            var productDocument = new ProductDocument(1, text);

            await testIndex.IndexDocuments(new[] { productDocument });

            // TermVectors will return us the indexed tokens for our field
            var result = await _fixture.OpenSearchClient.TermVectorsAsync<ProductDocument>(selector => selector
                    .Index(testIndex.Name)
                    .Document(productDocument)
                );

            result.IsValid.Should().BeTrue();
            var tokensAndFrequency = string.Join(", ", result.TermVectors.Values.SelectMany(value => value.Terms.Select(term => $"{term.Key}:{term.Value.TermFrequency}")));
            tokensAndFrequency.Should().BeEquivalentTo(expectedTokensCsv, explanation);
        }

        [Theory]
        /// <summary>
        /// Performs a term query on text mapping to observe the results
        /// </summary>
        [
            InlineData(
            "product",
            "The standard analyzer does not produce additional tokens for single word strings")
        ]
        [
            InlineData(
            "great product",
            "Two individual tokens are produced by the standard analyzer as it provides grammar based tokenisation")
        ]
        [
            InlineData(
            "This is a really amazing product. It is great, you absolutely must buy it!",
            "Many tokens are produced. Gramma is stripped so that only words are indexed. Case is normalised to lowercase. Recurring words are counted against the same token.")]
        public async Task TextMapping_TermQuery_DoesSomething(string text, string explanation)
        {
            await using var testIndex = await _fixture.CreateTestIndex<ProductDocument>(mapping => mapping
                    .Properties<ProductDocument>(propertyDescriptor => propertyDescriptor
                        .Text(word => word
                            .Name(name => name.Description)
                    )));

            var productDocument = new ProductDocument(1, text);

            await testIndex.IndexDocuments(new[] { productDocument });

            // TermVectors will return us the indexed tokens for our field
            var result = await _fixture.OpenSearchClient.SearchAsync<ProductDocument>(selector => selector
                    .Index(testIndex.Name)
                    .Query(query => query
                        .Term(term => term
                            .Field(field => field.Description)
                            .Value(text)
                            )
                        )
                    );

            result.IsValid.Should().BeTrue();
            result.Hits.Should().ContainSingle(explanation);
        }
    }
}