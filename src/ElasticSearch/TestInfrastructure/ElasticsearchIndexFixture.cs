using Elasticsearch.Net;
using Nest;

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

        public delegate Task PerformActionOnIndex(string testIndexName, IElasticClient opensearchClient);

        public async Task PerformActionInTestIndex<T>(
        string indexName,
        Func<TypeMappingDescriptor<T>, ITypeMapping> mappingDescriptor,
        PerformActionOnIndex action,
        Func<IndexSettingsDescriptor, IPromise<IIndexSettings>>? settingsDescriptor = null
        ) where T : class
        {
            var uniqueIndexName = indexName + Guid.NewGuid().ToString();
            try
            {
                var createIndexDescriptor = new CreateIndexDescriptor(uniqueIndexName)
                    .Map(mappingDescriptor);

                if (settingsDescriptor != null)
                {
                    createIndexDescriptor.Settings(settingsDescriptor);
                }

                var indexCreationResult = await ElasticClient.Indices.CreateAsync(uniqueIndexName, descriptor => createIndexDescriptor);

                if (!indexCreationResult.IsValid)
                {
                    throw new Exception($"Failed to create index {indexCreationResult.DebugInformation}");
                }

                await action(uniqueIndexName, ElasticClient);
            }
            finally
            {
                try
                {
                    await ElasticClient.Indices.DeleteAsync(uniqueIndexName);
                }
                catch (Exception ex)
                {
                    // Swallow the exception here - we tried our best to tidy up
                }
            }
        }
    }
}
