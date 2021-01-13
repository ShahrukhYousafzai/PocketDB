using System;
using ShahrukhYousafzai.PocketDB;

namespace PocketDBSample
{
    class Program
    {
        static void Main(string[] args)
        {
            //opening / creating
            PocketDatabase DB = new PocketDatabase("SampleData"); 

            //saving & loading
            DB.Set("sample", "hello world");
            string savedValue = DB.Get<string>("sample");

            Console.WriteLine(savedValue);
            Console.ReadLine();

            //closing
            DB.Close();

        }
    }
}
