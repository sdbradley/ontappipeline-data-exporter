using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LogglyTester
{
    class Program
    {
        static void Main(string[] args)
        {
            ILog logger = LogManager.GetLogger(typeof(Program));
            logger.Error("******AAA***AAA***AAA********");
            Console.Read();
        }
    }
}
