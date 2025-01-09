## Implement Change Stream in Azure Cosmos DB for Mongo API

### Purpose
This example demonstrates how to use the Change Stream to interact with documents as they are inserted or updated.

Deletes are not currently supported and instead a soft delete flag should be implemented.

Two common use cases include zero downtime data migration and change events.

### Dependencies
`dotnet add package MongoDB.Driver`

`dotnet add package Microsoft.Extensions.Configuration.UserSecrets`

### How Change Streams work
Change streams work like the change feed for the NoSQL API. Think of the Change Stream as a view of your container sorted by the timestamp of a document (`_ts` field).

This example processes the documents serially. Parallel processing is support but not covered in this example. See the documentation link for more information:

https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/custom-commands#parallel-change-stream

Mongo DB documentation can provide useful information as well. But, keep in mind the implementation on Cosmos DB varies.

https://www.mongodb.com/docs/manual/changeStreams/

The officiel Microsoft documentation can be found here:

https://learn.microsoft.com/en-us/azure/cosmos-db/mongodb/change-streams?tabs=csharp#changes-within-a-single-shard


### Secure storage of connection string
To securely store your Cosmos DB for Mongo connection string, we use `dotnet user-secrets`

Initialize the secrets store

`dotnet user-secrets init`

Add the secret

`dotnet user-secrets set "COSMOS_CONNECTION_STRING" "<connection string>"`

### Rotating the resume token
The resume token may not exist on first run. This application will start the change stream from today minus 100 days. This value can be edited to fit your use case.

Every time a document is read from the Change Stream, a new resume token is produced. The new resume token is saved to a collection for storage and the old token is deleted. This allows for easily resuming the change stream where your application left off should the application stop.

In this example, deleteOne() is used to delete the old resume token. However, it is more efficient to user TTL to delete the documents.

To implement this, you must configure a TTL index on the changeStreamToken collection. Set the expire time to -1 to not expire documents by default. Then, set the ttl property on documents to the designated value (60 seconds for instance).

A host builder should also be used to help the application exit gracefully allowing resume tokens to be written before exiting. If the application fails unexpectedly, there may be a duplicate document error thrown which you could ignore. This may also create orphaned resume tokens.

### Developer Notes

The database and collection names are hard coded. This can easily be moved to environment variables for a more secure and reusable configuration.

There are 3 collections: the source collection ie. dataCollection, the destination collection ie. newDataCollection, and the changeStreamToken collection.