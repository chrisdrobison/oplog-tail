using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using ServiceStack.Text;

namespace OplogTail
{
    public class TailService : TaskService
    {
        private static readonly ILog Log = LogManager.GetLogger<TailService>();
        private const string Namespace = "prmonline.projects";

        public TailService()
        {
            CancelTokenSource.Token.Register(OnStop);
        }

        public override string Name
        {
            get { return "Tail service"; }
        }

        private static string ConnectionString
        {
            get { return ConfigurationManager.ConnectionStrings["DbConnection"].ConnectionString; }
        }

        protected override void DoStart()
        {
            while (true)
            {
                try
                {
                    CancelTokenSource.Token.ThrowIfCancellationRequested();

                    Task.Factory.StartNew(TailOpLog,
                        CancelTokenSource.Token,
                        TaskCreationOptions.LongRunning,
                        TaskScheduler.Current)
                        .Wait();
                }
                catch (OperationCanceledException)
                {
                    Log.Info("Project indexing service operation cancelled");
                }
                catch (Exception ex)
                {
                    Log.FatalFormat("Project indexing service encountered a fatal error: {0}", ex, ex.Message);
                }

                if (CancelTokenSource.Token.IsCancellationRequested)
                {
                    break;
                }

                // Sleep to prevent thrashing the service
                Log.InfoFormat("Restarting {0} after sleep", Name);
                Thread.Sleep(2000);
            }
            Log.Info("Tail shut down");
        }

        private void TailOpLog()
        {
            while (true)
            {
                try
                {
                    CancelTokenSource.Token.ThrowIfCancellationRequested();
                    Log.InfoFormat("Tailing oplog on {0}", ConnectionString);

                    // Oplog includes operations that occur in all databases in the replicaset. The oplog denotes which
                    // database an operation goes to be using a namespace syntax
                    var client = new MongoClient(ConnectionString);
                    var collection = client.GetServer().GetDatabase("local").GetCollection("oplog.rs");

                    var query = Query.And(Query.GT("ts", new BsonTimestamp((int)DateTime.Today.ToUnixTime(), 0)),
                        Query.EQ("ns", Namespace));
                    var cursor = collection.Find(query)
                        .SetFlags(QueryFlags.TailableCursor | QueryFlags.AwaitData)
                        .SetSortOrder(SortBy.Ascending("$natural"));

                    using (var enumerator = new MongoCursorEnumerator<BsonDocument>(cursor))
                    {
                        while (true)
                        {
                            CancelTokenSource.Token.ThrowIfCancellationRequested();
                            Log.Debug("Enumerating");
                            if (enumerator.MoveNext())
                            {
                                var document = enumerator.Current;
                                Log.DebugFormat("Processing change: {0}", document);
                            }
                            else
                            {
                                Log.Debug("Didn't get anything");
                                if (enumerator.IsDead)
                                {
                                    Log.Info("Cursor has ended and is dead");
                                    break;
                                }

                                if (!enumerator.IsServerAwaitCapable)
                                {
                                    CancelTokenSource.Token.ThrowIfCancellationRequested();
                                    Log.Info("Throttling next cursor attempt. Awaiting 1 second.");
                                    Thread.Sleep(TimeSpan.FromMilliseconds(1000));
                                }
                                else
                                {
                                    // The method we are using for enumerating causes the loop execute as fast as it can
                                    // Slowing it down here a bit.
                                    Thread.Sleep(TimeSpan.FromMilliseconds(1000));
                                }
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Log.Info("Op log tailing operation cancelled");
                    return;
                }
                catch (Exception ex)
                {
                    Log.Error(ex.Message, ex);
                }
                finally
                {
                    if (!CancelTokenSource.Token.IsCancellationRequested)
                    {
                        Thread.Sleep(TimeSpan.FromMilliseconds(2000));
                    }
                }
            }
        }

        private void OnStop()
        {
            Log.Info("Stop called");
        }
    }
}