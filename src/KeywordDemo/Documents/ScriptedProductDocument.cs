namespace KeywordDemo.Documents
{
    /// <summary>
    /// A sample document that contains a single keyword field that is explored during multiple tests within the suite
    /// </summary>
    public record ScriptedProductDocument : ProductDocument
    {
        public ScriptedProductDocument(int id, string name) : base(id, name) { }

        public string Category { get; init; }
    }
}
