namespace ElasticSearchKeywordDemo.Documents
{
    /// <summary>
    /// An extension of the keyword product document that is used to deserialise an additional scripted field in the response
    /// </summary>
    public record ScriptedProductDocument : ProductDocument
    {
        public ScriptedProductDocument(int id, string name) : base(id, name) { }

        public string Category { get; init; }
    }
}
