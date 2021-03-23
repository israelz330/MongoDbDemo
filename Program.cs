using System;
using System.Collections.Generic;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace MongoDbDemo
{
    public class Program
    {
         public class MongoDBContext
        {
            private readonly IMongoClient _mongoDbClient = null;
            private readonly IMongoDatabase _mongoDB = null;

            public MongoDBContext()
            {
                _mongoDbClient = new MongoClient();
                _mongoDB = _mongoDbClient.GetDatabase("Mileiru");
            }         

            public IAggregateFluent<T> Aggregate<T>() => _mongoDB.GetCollection<T>(nameof(T)).Aggregate();

        }
        private static readonly MongoDBContext _db = new MongoDBContext();
        static void Main(string[] args)
        {
            var result = _db.Aggregate<ClientModel>()
            .Match(e => e.Age == 30)
            // .Group(e => e.Age, g => new{Key = g.Key, Count = g.Count(), Average = g.Average(e => e.Age)})
            .ToList();

            foreach (var item in result)
            {
                System.Console.WriteLine(item);
            }

            // MongoCRUD db = new MongoCRUD("Company");

            /* THIS WILL INSERT A NEW RECORD (BSON FORMAT)*/
            // db.InsertRecord("Clientes", new ClientModel{
            //     Name="Mercedes Muñiz",
            //     Address= new AddressModel{
            //         Street="4ta Privada Pedro Montoya",
            //         ZIP=78000,
            //         City="SLP"
            //     },
            //     Age= 20,
            //     Birthday = DateTime.Now
            // });



            // foreach(var rec in recs)
            // {
            //     Console.WriteLine($"| {rec.Name} |");
            // }



            // foreach(var rec in recs)
            // {
            //     Console.WriteLine($"| {rec.id} | {rec.Name} | {rec.Address.Street}");
            // }

            // var oneRec = db.LoadRecordsByID<ClientModel>("Clientes", new Guid("762fc179-4b23-4b0d-baba-2bbdafbda479"));
            // Console.WriteLine($"One rec = {oneRec.Name}");
            
            // oneRec.Birthday = new DateTime(1956, 06, 19,0,0,0, DateTimeKind.Utc);
            // db.UpsertRecord("Clientes", oneRec.id, oneRec);

            // db.DeleteRecord<ClientModel>("Clientes", oneRec.id);

            // Console.WriteLine($"Added DateOfBirth: {oneRec.Birthday}");
            
            
            Console.WriteLine($"DONE");
        }
    }

    public class ClientModel
    {
        [BsonId] //_id THIS IS THE UNIQUE ID FOR THE RECORD (Mongo automatically creates one for you)
        public Guid id { get; set; }
        public string Name { get; set; }
        public AddressModel Address { get; set; }
        public int Age { get; set; }
        public DateTime Birthday { get; set; }
    }

    [BsonIgnoreExtraElements] //BECAUSE THE NAME MODEL DOES NOT MATCH WITH THE "CLIENTES MODEL" TABLE. (WORKS FINE)
    public class NameModel
    {
        [BsonId] //_id THIS IS THE UNIQUE ID FOR THE RECORD (Mongo automatically creates one for you)
        public Guid id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
    }
    public class AddressModel
    {
        public string Street { get; set; }
        public int ZIP { get; set; }
        public string City { get; set; }
    }
    public class MongoCRUD
    {
        private IMongoDatabase db;

        public MongoCRUD(string database)
        {
            /* CONNECTION TO DATABSE*/
            var client = new MongoClient();
            db = client.GetDatabase(database);
        }

        public void InsertRecord<T>(string table, T record)
        {
            var collection = db.GetCollection<T>(table);
            collection.InsertOne(record);
        }

        public List<T> LoadRecords<T>(string table)
        {
            var collection = db.GetCollection<T>(table);
            return collection.Find(new BsonDocument()).ToList();
        }

        public T LoadRecordsByID<T>(string table, Guid id)
        {
            var collection = db.GetCollection<T>(table);
            var filter = Builders<T>.Filter.Eq("id", id);

            return collection.Find(filter).First();
        }

        [Obsolete]
        public void UpsertRecord<T>(string table, Guid id, T record)
        {
            var collection = db.GetCollection<T>(table);
            //IF YOU FIND AN ID THAT MATCHES THE ID PASSED, DELETE THAT RECORD
            //AND PUT THIS RECORD IN. OTHERWISE, INSERT THE RECORD ANYWAY.
            var result = collection.ReplaceOne(
                new BsonDocument("_id", id), 
                record, 
                new UpdateOptions{IsUpsert = true}
            );
        }

        public void DeleteRecord<T>(string table, Guid id)
        {
            var collection = db.GetCollection<T>(table);
            var filter = Builders<T>.Filter.Eq("id", id);
            collection.DeleteOne(filter);
        }

        public void AggregateRecord<T>(string table, T registro)
        {
            var collection = db.GetCollection<T>(table);

            var match = new BsonDocument 
                { 
                    { 
                        "$match", 
                        new BsonDocument 
                            {
                                {"Name", "Israel Zapata"} 
                            } 
                    }
                };

            var pipeline = new[] {match};

            var result = collection.Aggregate(PipelineDefinition<T, BsonDocument>.Create(match));

            // var result = collection.Aggregate<BsonDocument>(pipeline).ToListAsync();

            System.Console.WriteLine(result);
            
            }
        }

   

    }

