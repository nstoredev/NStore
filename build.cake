#addin "Cake.SqlTools"

//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////
var target = Argument("target", "Default");
var configuration = Argument("configuration", "Release");

//////////////////////////////////////////////////////////////////////
// PREPARATION
//////////////////////////////////////////////////////////////////////
var msSqlServerConnectionString = EnvironmentVariable("NSTORE_MSSQL") ?? "Server=localhost,1433;User Id=sa;Password=NStoreD0ck3r";
var msSqlDatabaseConnectionString  = msSqlServerConnectionString +";Database=NStore";

private void RunTest(string testProject, IDictionary<string,string> env = null)
{
    var projectDir = "./src/"+ testProject + "/";
    var settings = new ProcessSettings
    {
//        Arguments = "xunit -parallel none",
        Arguments = "xunit",
        WorkingDirectory = projectDir,
        EnvironmentVariables = env
    };

    StartProcess("dotnet", settings);
}

// Define Settings.
var artifactsDir    = Directory("./artifacts");
var solution        = "./src/NStore.sln";

//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////

Task("Clean")
    .Does(() =>
{
    CleanDirectory(artifactsDir);
});

Task("restore-packages")
    .IsDependentOn("Clean")
    .Does(() =>
{
    DotNetCoreRestore(solution);
   // NuGetRestore(solution);
});

Task("Build")
    .IsDependentOn("restore-packages")
    .Does(() =>
{
    var settings = new DotNetCoreBuildSettings
	{
		Configuration = configuration
	};

	DotNetCoreBuild(solution, settings);
});

Task("TestMsSql")
    .IsDependentOn("RunLibraryTests")
    .Does(()=>
{
var sql = @"USE master
IF EXISTS(select * from sys.databases where name='NStore')
DROP DATABASE NStore

CREATE DATABASE NStore";

    ExecuteSqlQuery(sql, new SqlQuerySettings()
    {
        Provider = "MsSql",
        ConnectionString = msSqlServerConnectionString
    });
    
    var env = new Dictionary<string, string>{
        { "NSTORE_MSSQL", msSqlDatabaseConnectionString},
    };
    
    RunTest("NStore.Persistence.MsSql.Tests",env);
});

Task("TestMongoDb")
    .IsDependentOn("RunLibraryTests")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "NSTORE_MONGODB", "mongodb://localhost/nstore-tests"},
    };

    RunTest("NStore.Persistence.Mongo.Tests",env);
});

Task("TestInMemory")
    .IsDependentOn("RunLibraryTests")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "xx", "val"},
    };

    RunTest("NStore.Persistence.Tests",env);
});


Task("TestSample")
    .IsDependentOn("RunLibraryTests")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "xx", "val"},
    };

    RunTest("NStore.Sample.Tests",env);
});

Task("RunLibraryTests")
    .IsDependentOn("Build")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "xx", "yy"},
    };

    RunTest("NStore.Tests",env);
});


Task("RunTests")
    .IsDependentOn("TestInMemory")
    .IsDependentOn("TestMongoDb")
    .IsDependentOn("TestMsSql")
    .IsDependentOn("TestSample")
    .Does(() =>
{
});


//////////////////////////////////////////////////////////////////////
// TASK TARGETS
//////////////////////////////////////////////////////////////////////

Task("Default")
    .IsDependentOn("RunTests");

//////////////////////////////////////////////////////////////////////
// EXECUTION
//////////////////////////////////////////////////////////////////////

RunTarget(target);
