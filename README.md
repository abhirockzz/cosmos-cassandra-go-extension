# Go extension for Azure Cosmos DB Cassandra API

This is a small library that implements [Retry Policy](https://pkg.go.dev/github.com/gocql/gocql?tab=doc#RetryPolicy) in the [gocql Cassandra driver](https://github.com/gocql/gocql) to intercept and handle Rate Limiting errors from the [Cassandra API in Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/cassandra-introduction). It resembles the implementation in the [Azure Cosmos Cassandra Extensions for Java](https://github.com/Azure/azure-cosmos-cassandra-extensions) project

To use it, create an instance of the policy and associate it with the `Query` or the `ClusterConfig` (at a global level)

```go
policy := retry.NewCosmosRetryPolicy(3)

....
//use it with cluster config
clusterConfig := gocql.NewCluster(cosmosCassandraContactPoint)
clusterConfig.RetryPolicy = retry.NewCosmosRetryPolicy(3)

....
//or each query
err := cs.Query(insertQuery).Bind(id, amount, state, time.Now()).Retry(policy).Exec()
```

For an example of how to use this, please see this sample project - github.com/abhirockzz/cosmos-rate-limiting (coming soon)

> Disclaimer: this is a purely experimental (personal) project and not an officially supported Microsoft library

### TODO

- failover/load-balancing extension