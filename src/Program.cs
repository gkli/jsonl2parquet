using System;
using System.Collections.Generic;

namespace Json2Parquet
{
    class Program
    {
        private static class Arguments
        {
            public const string SourceFile = "src";
            public const string DestinationFile = "dst";
            public const string Help = "help";
        }

        static void Main(string[] args)
        {
            var argIndex = IndexArguments(args);

            if (argIndex.ContainsKey(Arguments.Help))
            {
                if (bool.TryParse(argIndex[Arguments.Help], out bool isHelpNeeded))
                {
                    if (isHelpNeeded)
                    {
                        ShowHelp();
                        return;
                    }
                }
            }

            if (false == ValidateRequiredArguments(argIndex))
            {
                return;
            }


            string srcFile = argIndex[Arguments.SourceFile];
            string dstFile = argIndex[Arguments.DestinationFile];

            Console.WriteLine($"Converting '{srcFile}' to '{dstFile}'");
            System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
            watch.Start();
            Converter.ToParquet(srcFile, dstFile).Wait();
            watch.Stop();
            Console.WriteLine($"Conversion completed in {watch.Elapsed.TotalSeconds} seconds.");
            //Console.WriteLine("Hello World!");

            //string srcFile = @"C:\Users\gaklions\Downloads\gk-sample.json", dstFile = @"C:\Users\gaklions\Downloads\gk-sample.parquet";

            //Converter.ToParquet(srcFile, dstFile).Wait();

            //long? l = 12245;
            //double? d = 2342.21;

            //l = d;

            //d = l;
        }

        private static Dictionary<string, string> IndexArguments(string[] args)
        {
            Dictionary<string, string> dict = new Dictionary<string, string>();

            for (int i = 0; i < args.Length; i++)
            {
                string a = args[i];
                string arg = a.ToLower();

                if (arg.ToLower() == "-h" || arg.ToLower() == "-help")
                    dict.Add(Arguments.Help, true.ToString());
                else if (arg.StartsWith("-src ") || arg.StartsWith("-source ") || arg.StartsWith("-s"))
                {
                    dict.Add(Arguments.SourceFile, ParseFileName(args, i + 1));
                }
                else if (arg.StartsWith("-out ") || arg.StartsWith("-output ") || arg.StartsWith("-o"))
                {
                    dict.Add(Arguments.DestinationFile, ParseFileName(args, i + 1));
                }
            }

            return dict;
        }
        private static string ParseFileName(string[] args, int index)
        {
            if (index >= args.Length) return null;
            return args[index];
        }
        private static bool ValidateRequiredArguments(Dictionary<string, string> args)
        {
            bool areRequiredArgsMissing = false;
            System.Text.StringBuilder txt = new System.Text.StringBuilder();

            if (false == args.ContainsKey(Arguments.SourceFile))
            {
                txt.AppendLine("Source file name is required but was not specified");
                areRequiredArgsMissing = true;
            }
            else
            {
                try
                {
                    if (false == System.IO.File.Exists(args[Arguments.SourceFile]))
                    {
                        txt.AppendLine($"Source file '{args[Arguments.SourceFile]}' does not exist or you don't have permissions to access it.");
                        areRequiredArgsMissing = true;
                    }
                }
                catch
                {
                    //invalid file name
                    txt.AppendLine($"Source file '{args[Arguments.SourceFile]}' is not a valid file name.");
                    areRequiredArgsMissing = true;
                }
            }

            if (false == args.ContainsKey(Arguments.DestinationFile))
            {
                txt.AppendLine("Destination file name is required but was not specified");
                areRequiredArgsMissing = true;
            }
            else
            {
                try
                {
                    if (false == System.IO.File.Exists(args[Arguments.DestinationFile])) //check for valid destination file name
                    {
                        //txt.AppendLine($"Source file '{args[Arguments.SourceFile]}' does not exist or you don't have permissions to access it.");
                        //areRequiredArgsMissing = true;
                    }
                }
                catch
                {
                    //invalid file name
                    txt.AppendLine($"Destination file '{args[Arguments.DestinationFile]}' is not a valid file name.");
                    areRequiredArgsMissing = true;
                }
            }

            if (areRequiredArgsMissing)
            {
                //txt.Append("-h for help");

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("INPUT ERROR");

                Console.ResetColor();
                Console.WriteLine(txt.ToString());

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("-h for help");
                Console.ResetColor();
            }

            return !areRequiredArgsMissing;
        }

        private static void ShowHelp()
        {
            string commandName = System.IO.Path.GetFileNameWithoutExtension(System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName);

            Console.WriteLine($"{commandName} -src [source file name] -out [output file name]");
            Console.WriteLine("");
            Console.WriteLine("-src Fully qualified name of the source JSON-Line file. -s|-source");
            Console.WriteLine("");
            Console.WriteLine("-out Fully qualified name of the output PARQUET file. -o|-output");

            Console.WriteLine("SHOW HELP");
        }
    }
}
