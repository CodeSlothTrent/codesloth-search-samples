namespace ElasticSearchKeywordDemo.Documents
{
    public record ElasticsearchUserFavouriteProducts
    {
        public ElasticsearchUserFavouriteProducts(int userId, string[] productNames)
        {
            UserId = userId;
            ProductNames = productNames;
        }

        public int UserId { get; init; }
        public string[] ProductNames { get; init; }
    }
}
