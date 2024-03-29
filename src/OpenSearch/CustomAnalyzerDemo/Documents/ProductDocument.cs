namespace OpenSearchTextDemo.Documents
{
    /// <summary>
    /// A sample document that contains a single keyword field that is explored during multiple tests within the suite
    /// </summary>
    public record ProductDocument
    {
        public ProductDocument(int id, string description)
        {
            Id = id;
            Description = description ?? throw new ArgumentNullException(nameof(description));
        }

        /// <summary>
        /// The Id field of a document is automatically used for the document id at indexing time
        /// </summary>
        public int Id { get; init; }

        /// <summary>
        /// The string property of this document will be mapped as Text
        /// Conceptually this property could represent a description of a product
        /// </summary>
        public string Description { get; init; }
    }
}
