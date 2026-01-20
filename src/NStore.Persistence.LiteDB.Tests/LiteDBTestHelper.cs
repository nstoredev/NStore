using System.IO;

namespace NStore.Persistence.Tests
{
    public static class LiteDBTestHelper
    {
        /// <summary>
        /// Deletes LiteDB data files and log files for testing purposes.
        /// </summary>
        /// <param name="connectionString">The connection string (file path) to the LiteDB database.</param>
        public static void DeleteDataFiles(string connectionString)
        {
            // Delete data file
            FileUtils.SafeDelete(connectionString);

            // Delete log file
            var logFileName = Path.Combine
            (
                Path.GetDirectoryName(connectionString),
                Path.ChangeExtension
                (
                    Path.GetFileNameWithoutExtension(connectionString) + "-log",
                    Path.GetExtension(connectionString)
                )
            );

            FileUtils.SafeDelete(logFileName);
        }
    }
}
