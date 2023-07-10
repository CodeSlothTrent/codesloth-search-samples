namespace ElasticSearchKeywordDemo.Documents
{
    public record UserFavouriteProducts
    {
        public UserFavouriteProducts(int userId, string[] productNames)
        {
            UserId = userId;
            ProductNames = productNames;
        }

        public int UserId { get; init; }
        public string[] ProductNames { get; init; }
    }
}
