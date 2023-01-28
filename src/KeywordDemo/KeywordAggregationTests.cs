using KeywordDemo.Documents;
using OpenSearch.Client;
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

        /// <summary>
        /// Consider if you require eager global ordinals when using terms aggregations
        /// https://www.elastic.co/guide/en/elasticsearch/reference/current/eager-global-ordinals.html#_what_are_global_ordinals
        /// </summary>
        [Fact]
        public async Task KeywordMapping_CanBeUsedForTermsAggregation()
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
                            .Terms(productCounts, selector => selector.Field(field => field.Name))
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
                            .Cardinality(distinctProductTypes, selector => selector.Field(field => field.Name))
                        )
                    );

                    // Extract the total number of distinct product names
                    result.IsValid.Should().BeTrue();
                    var distinctProductCount = result.Aggregations.Cardinality(distinctProductTypes).Value;
                    distinctProductCount.Should().Be(2);
                }
            );
        }

        [Fact]
        public async Task KeywordMapping_CanBeUsedForMetricAggregation_TopHits()
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

                    const string productTypes = "productTypes";
                    const string topType = "topType";

                    var result = await opensearchClient.SearchAsync<ProductDocument>(selector => selector
                           .Index(uniqueIndexName)
                           .Query(query => query.MatchAll())
                           // We do not want any documents returned; just the aggregations
                           .Size(0)
                           .Aggregations(aggregations => aggregations
                            // Calculate terms of names
                            .Terms(productTypes, terms => terms.Field(field => field.Name)
                                .Aggregations(aggs => aggs
                                    // Extract the top document of each term aggregate bucket, sorted by id descending
                                    .TopHits(topType, selector => selector
                                    .Sort(sort => sort.Descending(desc => desc.Id))
                                    // Setting the size to 1 will only pull out the document with the highest id against the term
                                    .Size(1)
                                    ))
                        ))
                    );

                    // Our top hits documents are the documents with the highest id for their term
                    result.IsValid.Should().BeTrue();
                    var formattedResults = string.Join(", ", result
                        .Aggregations.Terms(productTypes).Buckets
                            .Select(bucket => bucket
                                .TopHits(topType)
                                .Documents<ProductDocument>()
                                .Select(doc => $"{doc.Id}:{doc.Name}")
                                // There is only one per term
                                .First()
                        ));

                    formattedResults.Should().BeEquivalentTo("5:mouse, 6:mouse pad");
                }
            );
        }
    }
}
