using OpenSearch.Client;
using OpenSearch.Net;

namespace OpenSearchTestInfrastructure
{
    public class OpenSearchIndexFixture
    {
        public IOpenSearchClient OpenSearchClient;
        public OpenSearchIndexFixture()
        {
            var clusterUri = new Uri("http://localhost:9200");
            var connectionSettings = new ConnectionSettings(clusterUri)
                .DisableDirectStreaming()
                .EnableDebugMode();

            OpenSearchClient = new OpenSearchClient(connectionSettings);
        }

        /// <summary>
        /// Please use a using reserved word when creating a variable for the test index to ensure it is disposed properly
        /// Creates an <see cref="ElasticsearchTestIndex"/> which automatically creates an index and tears it down at the end of the test
        /// </summary>
        public async Task<OpensearchTestIndex> CreateTestIndex<T>(
            Func<TypeMappingDescriptor<T>, ITypeMapping> mappingDescriptor,
            Func<IndexSettingsDescriptor, IPromise<IIndexSettings>>? settingsDescriptor = null
            )
            where T : class
        {
            var testIndex = new OpensearchTestIndex(OpenSearchClient);
            await testIndex.CreateIndex(mappingDescriptor, settingsDescriptor);
            return testIndex;
        }
    }
}
