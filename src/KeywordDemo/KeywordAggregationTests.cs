using KeywordDemo.Documents;
using TestInfrastructure;

namespace KeywordDemo
{
    public class KeywordAggregationTests : IClassFixture<IndexFixture>
    {
        private IndexFixture _fixture;

        public KeywordAggregationTests(IndexFixture fixture)
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

        [Fact]
        public async Task KeywordMapping_CanBeUsedForTermsAggregation_WithoutAnySpecialConsiderations()
        {
            var indexName = "keyword-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(3, "mouse pad"),
    new ProductDocument(4, "mouse"),
    new ProductDocument(5, "mouse"),
    new ProductDocument(6, "mouse pad"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    const string productCounts = "productCounts";

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(query => query.MatchAll())
                           // We do not want any documents returned; just the aggregations
                           .Size(0)
                           .Aggregations(aggregations => aggregations
                            .Terms(productCounts, termSelector => termSelector.Field(field => field.Name))
                        )
                    );

                    // Extract each term and its associated number of hits
                    result.IsValid.Should().BeTrue();
                    var formattedResults = string.Join(", ", result.Aggregations
                        .Terms(productCounts).Buckets
                        .Select(bucket => $"{bucket.Key}:{bucket.DocCount}")
                    );

                    formattedResults.Should().BeEquivalentTo("mouse:3, mouse pad:2");
                }
            );
        }

        [Fact]
        public async Task KeywordMapping_CanBeUsedForMetricAggregation_Cardinality()
        {
            var indexName = "keyword-index";
            await _fixture.PerformActionInTestIndex(
                indexName,
                mappingDescriptor,
                async (uniqueIndexName, opensearchClient) =>
                {
                    var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(3, "mouse pad"),
    new ProductDocument(4, "mouse"),
    new ProductDocument(5, "mouse"),
    new ProductDocument(6, "mouse pad"),
};

                    await _fixture.IndexDocuments(uniqueIndexName, productDocuments);

                    const string distinctProductTypes = "distinctProductTypes";

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(query => query.MatchAll())
                           // We do not want any documents returned; just the aggregations
                           .Size(0)
                           .Aggregations(aggregations => aggregations
                            .Cardinality(distinctProductTypes, termSelector => termSelector.Field(field => field.Name))
                        )
                    );

                    // Extract the total number of distinct product names
                    result.IsValid.Should().BeTrue();
                    var distinctProductCount = result.Aggregations.Cardinality(distinctProductTypes).Value;
                    distinctProductCount.Should().Be(2);
                }
            );
        }
    }
}
