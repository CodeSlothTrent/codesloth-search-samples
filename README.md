# Code Sloth Opensearch Tutorials
This public repository contains the .Net demo code discussed in OpenSearch articles on https://codesloth.blog.

## Solution Structure
The solution contains integration tests that are structured around specific OpenSearch concepts. 

It is currently being populated with tests that demonstrate how to map and use each of the Field Data Types.

```
Solution/
├─ FieldDataTypes/
│  ├─ KeywordDemo/
│  │  ├─ DemoFiles
│  ├─ TextDemo/
│  │  ├─ DemoFiles
├─ Infrastructure/
│  ├─ Fixtures

```

## Test Structure
Each test follows the same pattern:
- Use the test fixture to generate an ephemeral test-scoped index in a local OpenSearch cluster via a call to `PerformActionInTestIndex`
  - Tests may share a common mapping definition, given this may be the sole subject of exploration between tests
- Documents that pertain to the test are then indexed into the newly created index
  - All tests will create their own data. This keeps the scope of data minimal for each test and keeps the perceived complexity of each specific test minimal
- Mappings are pretty useless by themselves, so each test within a suite will then explore different ways that the mapping can be used
  - Search queries
  - Scripting
  - Aggregations
  - Etc
- Expectations are asserted on the action performed
- The test index is torn down
