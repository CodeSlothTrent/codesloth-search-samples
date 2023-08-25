﻿using Nest;

namespace ElasticSearchTestInfrastructure
{
    /// <summary>
    /// This test fixture lives for the duration of all tests within a class
    /// </summary>
    public class ElasticsearchIndexFixture
    {
        public IElasticClient ElasticClient;

        public ElasticsearchIndexFixture()
        {
            var clusterUri = new Uri("http://localhost:9200");
            var connectionSettings = new ConnectionSettings(clusterUri)
                .DisableDirectStreaming()
                .EnableApiVersioningHeader()
                .EnableDebugMode();

            ElasticClient = new ElasticClient(connectionSettings);
        }

        /// <summary>
        /// Please use a using reserved word when creating a variable for the test index to ensure it is disposed properly
        /// Creates an <see cref="ElasticsearchTestIndex"/> which automatically creates an index and tears it down at the end of the test
        /// </summary>
        public async Task<ElasticsearchTestIndex> CreateTestIndex<T>(
            Func<TypeMappingDescriptor<T>, ITypeMapping> mappingDescriptor,
            Func<IndexSettingsDescriptor, IPromise<IIndexSettings>>? settingsDescriptor = null
            )
            where T : class
        {
            var testIndex = new ElasticsearchTestIndex(ElasticClient);
            await testIndex.CreateIndex(mappingDescriptor, settingsDescriptor);
            return testIndex;
        }
    }
}
