using System.Threading;

namespace OplogTail
{
    public abstract class DefaultService
    {
        private readonly CancellationTokenSource _cancellationTokenSource;

        protected DefaultService()
        {
            _cancellationTokenSource = new CancellationTokenSource();
        }

        protected CancellationTokenSource CancelTokenSource
        {
            get { return _cancellationTokenSource; }
        }

        public abstract string Name { get; }

        public virtual void Start()
        {
            DoStart();
        }

        public virtual void Stop()
        {
            _cancellationTokenSource.Cancel();
        }

        protected abstract void DoStart();
    }
}