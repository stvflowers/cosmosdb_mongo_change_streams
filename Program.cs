using System.Reflection.Metadata;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;

namespace cosmos_mongo_changeStream;

class Program
{
    static void Main(string[] args)
    {

        /////////////////////////////////////////////////////
        /// Instantiate variables, get secrets, and create a client
        /// Variables
        string database = "db1";
        string dataCollection = "inputChangeStream";
        string newDataCollection = "outputChangeStream";
        string changeStreamCollection = "changeStreamTokens";
        /// Get Secrets
        var config = new ConfigurationBuilder().AddUserSecrets<Program>().Build();
        string? connectionString = config["COSMOS_CONNECTION_STRING"]; 
        /// Create a client
        var _client = new MongoClient(connectionString);

        /////////////////////////////////////////////////////
        //// CHANGE STREAM LISTENER
        //// Get a resume token for the entire collection
        var resumeToken = GetChangeStreamToken(_client, database, changeStreamCollection);
        //// Listen for changes on the change stream
        ListenChangeStream(_client, database, dataCollection, newDataCollection, changeStreamCollection, resumeToken);

    }

    /////////////////////////////////////////////////////
    //// Get a change token from database collection
    //// If a resume token is retrieved from the database, use it to resume the change stream
    //// Otherwise, start the change stream from a specific time
    public static BsonDocument? GetChangeStreamToken(IMongoClient _client, string database, string changeStreamCollection)
    {
        var tokenCollection = _client.GetDatabase(database).GetCollection<BsonDocument>(changeStreamCollection);
        var tokenDocument = tokenCollection.Find(new BsonDocument()).Sort("{_id: -1}").FirstOrDefault();
        //// If null, return null
        return tokenDocument?["resumeToken"]?["_id"].AsBsonDocument;
    }
    
    /////////////////////////////////////////////////////
    //// Start listening to the change stream
    public static void ListenChangeStream(IMongoClient _client, string database, string dataCollection, string newDataCollection, string changeStreamCollection, BsonDocument? resumeToken)
    {
        
        ChangeStreamOptions options;

        if (resumeToken != null)
        {
            options = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                ResumeAfter = resumeToken
            };

        } else {
            options = new ChangeStreamOptions
            {
                FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                StartAtOperationTime = new BsonTimestamp((DateTime.UtcNow.AddDays(-100) - new DateTime(1970, 1, 1)).Ticks / 10000)
            };
        }

        var inputChangeStreamCollection = _client.GetDatabase(database).GetCollection<BsonDocument>(dataCollection);

        var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
        .Match(change =>
            change.OperationType == ChangeStreamOperationType.Insert
            || change.OperationType == ChangeStreamOperationType.Update
            || change.OperationType == ChangeStreamOperationType.Replace
        )
        .AppendStage<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>, BsonDocument>(
            @"{ 
                $project: { 
                    '_id': 1, 
                    'fullDocument': 1, 
                    'ns': 1, 
                    'documentKey': 1 
                }
            }"
        );

        using IChangeStreamCursor<BsonDocument> enumerator = inputChangeStreamCollection.Watch(
            pipeline,
            options
        );

        Console.WriteLine("Listening for changes...");

        while (enumerator.MoveNext())
        {
            IEnumerable<BsonDocument> changes = enumerator.Current;
            
            // Initialize a variable to store the last change
            BsonDocument? lastChange = null;

            foreach(BsonDocument change in changes)
            {
                Console.WriteLine(change);
                
                // Write Document to new collection here
                BsonDocument? changeDocument = change["fullDocument"].AsBsonDocument;
                if (changeDocument != null)
                {
                    WriteToNewCollection(_client, database, newDataCollection, changeDocument);
                    // Save the new resume token to the collection
                    SaveChangeToken(_client, database, changeStreamCollection, change);
                    // Delete the old resume token after the new one has been saved
                    // Resume token will not be null if a token was retrieved from the database
                    // We then set it to null because resume tokens are being held in memory while the change stream runs
                    if (resumeToken != null)
                    {
                        DeleteChangeToken(_client, database, changeStreamCollection, resumeToken);
                        resumeToken = null;
                    } 
                    else if (lastChange != null) 
                    {
                        DeleteChangeToken(_client, database, changeStreamCollection, lastChange);
                    }
                }
                // Store the last change to delete when new change is written successfully
                lastChange = change;
            }  
        }
    }

    /////////////////////////////////////////////////////
    //// Save the latest change token to the specified collection
    public static void SaveChangeToken(IMongoClient _client, string database, string collection, BsonDocument resumeToken)
    {
        var tokenCollection = _client.GetDatabase(database).GetCollection<BsonDocument>(collection);
        tokenCollection.InsertOne(new BsonDocument { { "resumeToken", resumeToken } });
    }
    
    /////////////////////////////////////////////////////
    //// Delete the old resume token from the specified collection
    public static void DeleteChangeToken(IMongoClient _client, string database, string collection, BsonDocument resumeToken)
    {
        var tokenCollection = _client.GetDatabase(database).GetCollection<BsonDocument>(collection);
        tokenCollection.DeleteOne(new BsonDocument { { "resumeToken", resumeToken } });
    }
    
    /////////////////////////////////////////////////////
    //// Delete the old resume token from the specified collection
    public static void WriteToNewCollection(IMongoClient _client, string database, string collection, BsonDocument document)
    {
        var newCollection = _client.GetDatabase(database).GetCollection<BsonDocument>(collection);
        newCollection.InsertOne(document);
    }
    
    /////////////////////////////////////////////////////
    //// Unused method
    //// Get a resume token for each physical partition at date time now
    //// Used for parrellel processing of change stream
    public static void RunCommandToken(IMongoClient _client, string database, string collection)
    {
        
        var _database = _client.GetDatabase(database);
        
        // Custom command to get change stream tokens
        var command = new BsonDocument
        {
            { "customAction", "GetChangeStreamTokens" },
            { "collection", collection }
        };

        _client.GetDatabase(database).RunCommand<BsonDocument>(command);
        
        var result = _database.RunCommand<BsonDocument>(command);
        
        Console.WriteLine(result);

    }

}
