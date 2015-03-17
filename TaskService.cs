using System;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;

namespace OplogTail
{
    /// <summary>
    ///     Sub service that runs in a task
    /// </summary>
    public abstract class TaskService : DefaultService
    {
        private static readonly ILog Log = LogManager.GetLogger<TaskService>();
        private Task _task;

        protected TaskService()
        {
            _task = new Task(DoStart, CancelTokenSource.Token, TaskCreationOptions.LongRunning);
        }

        public override void Start()
        {
            _task.Start();
        }

        public override void Stop()
        {
            CancelTokenSource.Cancel();
            _task = _task
                .ContinueWith(
                    task =>
                    {
                        if (task.Exception != null)
                        {
                            task.Exception.Handle(
                                exception =>
                                {
                                    Log.Error(exception.Message, exception);
                                    return true;
                                });
                        }
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.None,
                    TaskScheduler.Default);
            try
            {
                _task.Wait();
            }
            catch (AggregateException ex)
            {
                Log.ErrorFormat(ex.Message, ex);
            }
        }
    }
}