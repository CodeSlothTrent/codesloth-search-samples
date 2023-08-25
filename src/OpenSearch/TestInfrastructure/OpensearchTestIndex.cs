using OpenSearch.Client;
using OpenSearch.Net;

namespace OpenSearchTestInfrastructure
{
    public class OpensearchTestIndex : IAsyncDisposable
    {
        private readonly IOpenSearchClient OpenSearchClient;
        public string Name { get; init; } = Guid.NewGuid().ToString();

        public OpensearchTestIndex(IOpenSearchClient elasticClient)
        {
            OpenSearchClient = elasticClient;
        }

        public async Task CreateIndex<T>(
            Func<TypeMappingDescriptor<T>, ITypeMapping> mappingDescriptor,
            Func<IndexSettingsDescriptor, IPromise<IIndexSettings>>? settingsDescriptor = null
        )
            where T : class
        {
            var createIndexDescriptor = new CreateIndexDescriptor(Name)
                    .Map(mappingDescriptor);

            if (settingsDescriptor != null)
            {
                createIndexDescriptor.Settings(settingsDescriptor);
            }

            var indexCreationResult = await OpenSearchClient.Indices.CreateAsync(Name, descriptor => createIndexDescriptor);

            if (!indexCreationResult.IsValid)
            {
                throw new Exception($"Failed to create index {indexCreationResult.DebugInformation}");
            }
        }

        public async Task IndexDocuments<T>(T[] docs) where T : class
        {
            var bulkIndexResponse = await OpenSearchClient.BulkAsync(selector => selector
                       .IndexMany(docs)
                       .Index(Name)
                       // We want to be able to search these doucments right away. Force a refresh
                       .Refresh(Refresh.True)
                   );

            if (!bulkIndexResponse.IsValid)
            {
                throw new Exception($"Failed to index documents. {bulkIndexResponse.DebugInformation}");
            }
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await OpenSearchClient.Indices.DeleteAsync(Name);
            }
            catch (Exception ex)
            {
                // Swallow the exception here - we tried our best to tidy up
            }

            GC.SuppressFinalize(this);
        }
    }
}
