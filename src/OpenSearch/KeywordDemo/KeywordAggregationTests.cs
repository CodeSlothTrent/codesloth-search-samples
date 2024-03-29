﻿using OpenSearchKeywordDemo.Documents;
using OpenSearchTestInfrastructure;

namespace OpenSearchKeywordDemo
{
    public class KeywordAggregationTests : IClassFixture<OpenSearchIndexFixture>
    {
        private OpenSearchIndexFixture _fixture;

        public KeywordAggregationTests(OpenSearchIndexFixture fixture)
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
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);

            var productDocuments = new[] {
                        new ProductDocument(1, "mouse"),
                        new ProductDocument(2, "mouse pad"),
                        new ProductDocument(3, "mouse"),
                        new ProductDocument(4, "mouse"),
                        new ProductDocument(5, "mouse pad"),
                    };

            await testIndex.IndexDocuments(productDocuments);

            const string productCounts = "productCounts";

            var result = await _fixture.OpenSearchClient.SearchAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
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

        [Fact]
        public async Task KeywordMapping_CanBeUsedForMetricAggregation_Cardinality()
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);

            var productDocuments = new[] {
                        new ProductDocument(1, "mouse"),
                        new ProductDocument(2, "mouse pad"),
                        new ProductDocument(3, "mouse"),
                        new ProductDocument(4, "mouse"),
                        new ProductDocument(5, "mouse pad"),
                    };

            await testIndex.IndexDocuments(productDocuments);

            const string distinctProductTypes = "distinctProductTypes";

            var result = await _fixture.OpenSearchClient.SearchAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
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

        /// <summary>
        /// Top hits is not recommended as a top level aggreagtion https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-top-hits-aggregation.html
        /// Group using collapse instead (test below) <seealso cref="KeywordMapping_CanBeUsedForTermAggregation_Collapse()"/>
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task KeywordMapping_CanBeUsedForTermAggregation_TopHits()
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);

            var productDocuments = new[] {
                        new ProductDocument(1, "mouse"),
                        new ProductDocument(2, "mouse pad"),
                        new ProductDocument(3, "mouse"),
                        new ProductDocument(4, "mouse"),
                        new ProductDocument(5, "mouse pad"),
                    };

            await testIndex.IndexDocuments(productDocuments);

            const string productTypes = "productTypes";
            const string topType = "topType";

            var result = await _fixture.OpenSearchClient.SearchAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
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

            formattedResults.Should().BeEquivalentTo("4:mouse, 5:mouse pad");
        }

        /// <summary>
        /// https://www.elastic.co/guide/en/elasticsearch/reference/current/collapse-search-results.html
        /// Collapse the results on a given field, extracting the top result based on the sorting criteria specified
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task KeywordMapping_CanBeUsedForTermAggregation_Collapse()
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);

            var productDocuments = new[] {
                        new ProductDocument(1, "mouse"),
                        new ProductDocument(2, "mouse pad"),
                        new ProductDocument(3, "mouse"),
                        new ProductDocument(4, "mouse"),
                        new ProductDocument(5, "mouse pad"),
                    };

            await testIndex.IndexDocuments(productDocuments);

            const string productTypes = "productTypes";
            const string topType = "topType";

            var result = await _fixture.OpenSearchClient.SearchAsync<ProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .Query(query => query.MatchAll())
                   // We do not want any documents returned; just the aggregations
                   //.Size(0)
                   .Collapse(selector => selector.Field(field => field.Name))
                   .Sort(sort => sort.Descending(field => field.Id))
                   .From(0)
            );

            // Our top hits documents are the documents with the highest id for their term
            result.IsValid.Should().BeTrue();
            var formattedResults = string.Join(", ", result.Documents.Select(doc => $"{doc.Id}:{doc.Name}"));
            formattedResults.Should().BeEquivalentTo("5:mouse pad, 4:mouse");
        }

        [Fact]
        public async Task KeywordMapping_CanBeUsedForAdjacencyMatrixAggregation()
        {
            await using var testIndex = await _fixture.CreateTestIndex<UserFavouriteProducts>(mapping => mapping
            .Properties<UserFavouriteProducts>(propertyDescriptor => propertyDescriptor
                .Keyword(word => word.Name(name => name.ProductNames))
            ));

            var userPurchasedProductDocuments = new[] {
                        new UserFavouriteProducts(1, new []{ "mouse", "mouse pad" }),
                        new UserFavouriteProducts(2, new []{ "mouse" }),
                        new UserFavouriteProducts(3, new []{ "keyboard" }),
                        new UserFavouriteProducts(4, new []{ "mouse pad", "keyboard" }),
                        new UserFavouriteProducts(5, new []{ "mouse", "keyboard" }),
                        new UserFavouriteProducts(6, new []{ "mouse", "mouse pad" }),
                    };

            await testIndex.IndexDocuments(userPurchasedProductDocuments);

            const string userProductPurchases = "userProductPurchases";

            var result = await _fixture.OpenSearchClient.SearchAsync<UserFavouriteProducts>(selector => selector
                   .Index(testIndex.Name)
                   .Query(query => query.MatchAll())
                   // We do not want any documents returned; just the aggregations
                   .Size(0)
                   .Aggregations(aggregations => aggregations
                    .AdjacencyMatrix(userProductPurchases, selector => selector
                        .Filters(filter => filter
                            .Filter("mouse", f => f.Terms(term => term.Field(f => f.ProductNames).Terms(new[] { "mouse" })))
                            .Filter("mouse pad", f => f.Terms(term => term.Field(f => f.ProductNames).Terms(new[] { "mouse pad" })))
                            .Filter("keyboard", f => f.Terms(term => term.Field(f => f.ProductNames).Terms(new[] { "keyboard" })))
                        )
                    )
                ));

            // Extract each term and its associated number of hits
            result.IsValid.Should().BeTrue();
            var formattedResults = string.Join(", ", result.Aggregations
                .AdjacencyMatrix(userProductPurchases).Buckets
                .Select(bucket => $"{bucket.Key}:{bucket.DocCount}")
            );

            formattedResults.Should().BeEquivalentTo("keyboard:3, keyboard&mouse:1, keyboard&mouse pad:1, mouse:4, mouse pad:3, mouse&mouse pad:2");
        }
    }
}
