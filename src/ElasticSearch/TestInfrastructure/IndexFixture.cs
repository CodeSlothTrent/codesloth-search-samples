using Elasticsearch.Net;
using Nest;

namespace ElasticSearchTestInfrastructure
{
    public class IndexFixture : IDisposable
    {
        private IElasticClient _elasticClient;
        public IndexFixture()
        {
            var clusterUri = new Uri("http://localhost:9200");
            var connectionSettings = new ConnectionSettings(clusterUri)
                .DisableDirectStreaming()
                .EnableApiVersioningHeader()
                .EnableDebugMode();

            _elasticClient = new ElasticClient(connectionSettings);
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

                var indexCreationResult = await _elasticClient.Indices.CreateAsync(uniqueIndexName, descriptor => createIndexDescriptor);

                if (!indexCreationResult.IsValid)
                {
                    throw new Exception($"Failed to create index {indexCreationResult.DebugInformation}");
                }

                await action(uniqueIndexName, _elasticClient);
            }
            finally
            {
                try
                {
                    await _elasticClient.Indices.DeleteAsync(uniqueIndexName);
                }
                catch (Exception ex)
                {
                    // Swallow the exception here - we tried our best to tidy up
                }
            }
        }

        /// <summary>
        /// An overload that takes a settings selector
        /// </summary>
        public async Task PerformActionInTestIndexWithSettings<T>(
          string indexName,
          Func<IndexSettingsDescriptor, IPromise<IIndexSettings>> settingsDescriptor,
          Func<TypeMappingDescriptor<T>, ITypeMapping> mappingDescriptor,
          PerformActionOnIndex action
          ) where T : class
        {
            await PerformActionInTestIndex(indexName, mappingDescriptor, action, settingsDescriptor);
        }

        public async Task IndexDocuments<T>(string indexName, T[] docs) where T : class
        {
            var bulkIndexResponse = await _elasticClient.BulkAsync(selector => selector
                       .IndexMany(docs)
                       .Index(indexName)
                       // We want to be able to search these doucments right away. Force a refresh
                       .Refresh(Refresh.True)
                   );

            if (!bulkIndexResponse.IsValid)
            {
                throw new Exception($"Failed to index documents. {bulkIndexResponse.DebugInformation}");
            }
        }

        public void Dispose()
        {
        }
    }
}
