using ElasticSearchKeywordDemo.Documents;
using Nest;

namespace ElasticsearchKeywordDemo.Indexing
{
    public class ElasticsearchKeywordIndexingTests
    {
        /// <summary>
        /// This test creates an index and a keyword mapping with all required logic in isolation
        /// Other tests for querying the keyword field use a more efficeint test fixture to allow each test to only focus on
        /// the logic for querying and not the test index setup
        /// </summary>
        [Theory]
        [InlineData("Mouse", "Single word is indexed exactly as given")]
        [InlineData("Mouse pad", "Two words are indexed exactly as given")]
        [InlineData("This is a sentence! It contains some, really bad. Grammar;", "All grammar is indexed exactly as given")]
        public async Task KeywordMapping_IndexesASingleTokenForGivenString(string termText, string explanation)
        {
            var clusterUri = new Uri("http://localhost:9200");
            var connectionSettings = new ConnectionSettings(clusterUri)
                .DisableDirectStreaming()
                .EnableApiVersioningHeader()
                .EnableDebugMode();

            var elasticClient = new ElasticClient(connectionSettings);

            const string indexName = "keyword-indexing";
            await elasticClient.Indices.CreateAsync(indexName, createIndexDescriptor => createIndexDescriptor.Map(
                mapping => mapping.Properties<ElasticsearchProductDocument>(
                    propertyDescriptor => propertyDescriptor.Keyword(
                        word => word.Name(name => name.Name)
                        )
                    )
                )
            );

            var productDocument = new ElasticsearchProductDocument(1, termText);

            await elasticClient.BulkAsync(selector => selector
                .Index(indexName)
                .IndexMany(new[] { productDocument })
                .Refresh(Elasticsearch.Net.Refresh.True)
            );

            var result = await elasticClient.TermVectorsAsync<ElasticsearchProductDocument>(selector => selector
                   .Index(indexName)
                   .Document(productDocument)
               );

            result.IsValid.Should().BeTrue();
            var tokensAndFrequency = string.Join(", ", result.TermVectors.Values.SelectMany(value => value.Terms.Select(term => $"{term.Key}:{term.Value.TermFrequency}")));
            var expectedTokenCsv = $"{termText}:1";
            tokensAndFrequency.Should().BeEquivalentTo(expectedTokenCsv, explanation);

            await elasticClient.Indices.DeleteAsync(indexName);
        }
    }
}
