using System;

namespace OplogTail
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var tailService = new TailService();
            tailService.Start();

            Console.Write("Press any key to exit...");
            Console.ReadLine();

            tailService.Stop();
        }
    }
}