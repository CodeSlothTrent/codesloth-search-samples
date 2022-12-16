using OpenSearch.Client;
using OpenSearch.Net;

namespace KeywordFilterType
{
    public class IndexFixture : IDisposable
    {
        private IOpenSearchClient _openSearchClient;
        public IndexFixture()
        {
            var clusterUri = new Uri("http://localhost:9200");
            var connectionSettings = new ConnectionSettings(clusterUri)
                .DisableDirectStreaming()
                .EnableDebugMode();

            _openSearchClient = new OpenSearchClient(connectionSettings);
        }

        public delegate Task PerformActionOnIndex(string testIndexName, IOpenSearchClient opensearchClient);

        public async Task PerformActionInTestIndex<T>(string indexName, Func<TypeMappingDescriptor<T>, ITypeMapping> mappingDescriptor, PerformActionOnIndex action) where T : class
        {
            var uniqueIndexName = indexName + Guid.NewGuid().ToString();
            try
            {
                var indexCreationResult = await _openSearchClient.Indices.CreateAsync(
                    uniqueIndexName,
                    createRequest => createRequest
                    .Map(mappingDescriptor)
                );

                if (!indexCreationResult.IsValid)
                {
                    throw new Exception($"Failed to create index {indexCreationResult.DebugInformation}");
                }

                await action(uniqueIndexName, _openSearchClient);
            }
            finally
            {
                try
                {
                    await _openSearchClient.Indices.DeleteAsync(uniqueIndexName);
                }
                catch (Exception ex)
                {
                    // Swallow the exception here - we tried our best to tidy up
                }
            }
        }

        public async Task IndexDocuments<T>(string indexName, T[] docs) where T : class
        {
            var bulkIndexResponse = await _openSearchClient.BulkAsync(selector => selector
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
