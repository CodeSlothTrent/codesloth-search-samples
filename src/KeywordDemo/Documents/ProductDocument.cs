namespace KeywordDemo.Documents
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

        public int Id { get; init; }

        public string Name { get; init; }
    }
}
