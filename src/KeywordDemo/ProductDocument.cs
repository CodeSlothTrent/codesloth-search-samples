namespace KeywordFilterType.Documents
{
    /// <summary>
    /// A sample document that represents a product that will have filter search applied to it via keywords
    /// </summary>
    public record ProductDocument
    {
        public ProductDocument(int id, string name, string category)
        {
            Id = id;
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Category = category ?? throw new ArgumentNullException(nameof(category));
        }

        public int Id { get; init; }

        public string Name { get; init; }

        public string Category { get; init; }
    }
}
