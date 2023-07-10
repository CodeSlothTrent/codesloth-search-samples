namespace ElasticSearchKeywordDemo.Documents
{
    /// <summary>
    /// A sample document that contains a single keyword field that is explored during multiple tests within the suite
    /// </summary>
    public record ProductDocument
    {
        public ProductDocument(int id, string name)
        {
            Id = id;
            Name = name ?? throw new ArgumentNullException(nameof(name));
        }

        /// <summary>
        /// The Id field of a document is automatically used for the document id at indexing time
        /// </summary>
        public int Id { get; init; }

        /// <summary>
        /// This string property will be mapped as a keyword
        /// Conceptually this property may represent the name of a product
        /// </summary>
        public string Name { get; init; }
    }
}
