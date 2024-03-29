using FluentAssertions;
using OpenSearch.Client;
using OpenSearchTestInfrastructure;
using OpenSearchTextDemo.Documents;

namespace OpenSearchKeywordDemo
{
    public class CustomAnalyzerTests : IClassFixture<OpenSearchIndexFixture>
    {
        private OpenSearchIndexFixture _fixture;

        public CustomAnalyzerTests(OpenSearchIndexFixture fixture)
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
        [Fact]
        public async Task CustomAnalyzer_ExecutesCharacterFilterThenTokenizerThenTokenFilters()
        {
            var indexName = "custom-analyzer";
            var testAnalyzerName = "test-analyzer";
            var customStopWordFilterName = "my_custom_stop_words_filter";
            var createIndexDescriptor = new CreateIndexDescriptor(indexName)
                .Settings(settings => settings
                    .Analysis(analysis => analysis
                        .Analyzers(analyzers => analyzers
                            .Custom(testAnalyzerName, descriptor => descriptor
                                .CharFilters("html_strip")
                                .Tokenizer("standard")
                                .Filters(customStopWordFilterName, "lowercase")
                            )
                        )
                        .TokenFilters(tokenFilters => tokenFilters
                            .Stop(customStopWordFilterName, filter => filter
                                .StopWords("/n")
                                )
                            )
                        )
                    )
                .Map<ProductDocument>(mapping => mapping
                    .Properties(properties => properties
                        .Text(text => text
                            .Analyzer(testAnalyzerName)
                            .Name(prop => prop.Description)
                            )
                        )
                    );


            var indexCreationResult = await _fixture.OpenSearchClient.Indices.CreateAsync(indexName, descriptor => createIndexDescriptor);

            // View the settings that define our custom analyzer
            var settingsRequest = new GetIndexSettingsRequest();
            var settingResult= await _fixture.OpenSearchClient.Indices.GetSettingsAsync(settingsRequest);

            // View where the custom analyzer has been mapped to documents
            var mappingRequest = new GetMappingRequest();
            var mappingResult = await _fixture.OpenSearchClient.Indices.GetMappingAsync(mappingRequest);

            var productDocument = new ProductDocument(1, "<b> Example input text </b>");

            var indexRequest = new IndexRequest<ProductDocument>(productDocument, indexName);
            await _fixture.OpenSearchClient.IndexAsync(indexRequest);

            var termVectorResult = await _fixture.OpenSearchClient.TermVectorsAsync<ProductDocument>(selector => selector
                   .Index(indexName)
                   .Document(productDocument)
               );

            // Each token is parsed from the response, against the number of times it appeared in the given string
            var tokensAndFrequency = termVectorResult.TermVectors.Values.SelectMany(value => value.Terms.Select(term => $"{term.Key}:{term.Value.TermFrequency}"));
            var expectedTokensAndFrequencies = new[] { "example:1", "input:1", "text:1" };
            tokensAndFrequency.Should().BeEquivalentTo(expectedTokensAndFrequencies, options => options.WithStrictOrdering());

            var deleteResponse = await _fixture.OpenSearchClient.Indices.DeleteAsync(indexName);
        }
    }
}
