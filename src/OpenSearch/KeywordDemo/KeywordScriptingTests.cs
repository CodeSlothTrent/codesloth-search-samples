using OpenSearchKeywordDemo.Documents;
using OpenSearchTestInfrastructure;

namespace OpenSearchKeywordDemo
{
    public class KeywordScriptingTests : IClassFixture<OpenSearchIndexFixture>
    {
        private OpenSearchIndexFixture _fixture;

        public KeywordScriptingTests(OpenSearchIndexFixture fixture)
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
        public async Task KeywordMapping_CanBeUsedToCreateAScriptedField()
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);
            var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};
            await testIndex.IndexDocuments(productDocuments);

            const string categoryFieldName = nameof(ScriptedProductDocument.Category);

            var result = await _fixture.OpenSearchClient.SearchAsync<ScriptedProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .ScriptFields(scriptFields => scriptFields
                    .ScriptField(
                       categoryFieldName,
                       selector => selector.Source($"doc['{nameof(ProductDocument.Name).ToLowerInvariant()}'].value == 'mouse' ? 'computer accessory' : 'mouse accessory'"))
                    )
                   .Source(true)
               );

            result.IsValid.Should().BeTrue();

            // Cannot get ValueOf<TDoc, TFieldType>() working at time of writing - it always returns null.
            foreach (var hit in result.Hits)
            {
                hit.Fields.ValueOf<ScriptedProductDocument, string>(doc => doc.Category).Should().BeNull();
            }

            // Using Value<TFieldType> with string lookup instead
            var formattedResults = string.Join(", ", result.Hits.Select(hit => $"{hit.Source.Name}:{hit.Fields.Value<string>(categoryFieldName)}"));
            formattedResults.Should().BeEquivalentTo("mouse:computer accessory, mouse pad:mouse accessory");
        }

        [Fact]
        public async Task KeywordMapping_CanBeUsedToCreateAScriptedField_FromAnInterpolatedVariable()
        {
            await using var testIndex = await _fixture.CreateTestIndex(mappingDescriptor);
            var productDocuments = new[] {
    new ProductDocument(1, "mouse"),
    new ProductDocument(2, "mouse pad"),
};

            await testIndex.IndexDocuments(productDocuments);

            const string categoryFieldName = nameof(ScriptedProductDocument.Category);

            var scriptedVariableValue = "mouse";

            var result = await _fixture.OpenSearchClient.SearchAsync<ScriptedProductDocument>(selector => selector
                   .Index(testIndex.Name)
                   .ScriptFields(scriptFields => scriptFields
                    .ScriptField(
                       categoryFieldName,
                       selector => selector.Source($"doc['{nameof(ProductDocument.Name).ToLowerInvariant()}'].value == '{scriptedVariableValue}' ? 'computer accessory' : 'mouse accessory'"))
                    )
                   .Source(true)
               );

            result.IsValid.Should().BeTrue();

            // Cannot get ValueOf<TDoc, TFieldType>() working at time of writing - it always returns null.
            foreach (var hit in result.Hits)
            {
                hit.Fields.ValueOf<ScriptedProductDocument, string>(doc => doc.Category).Should().BeNull();
            }

            // Using Value<TFieldType> with string lookup instead
            var formattedResults = string.Join(", ", result.Hits.Select(hit => $"{hit.Source.Name}:{hit.Fields.Value<string>(categoryFieldName)}"));
            formattedResults.Should().BeEquivalentTo("mouse:computer accessory, mouse pad:mouse accessory");
        }
    }
}
