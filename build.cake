#addin "Cake.SqlTools"

//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////
var target = Argument("target", "Default");
var testOutput = Argument("testoutput", "");
var configuration = Argument("configuration", "Release");

//////////////////////////////////////////////////////////////////////
// PREPARATION
//////////////////////////////////////////////////////////////////////
var msSqlServerConnectionString = RemoveQuotes(GetVariable("NSTORE_MSSQL_INSTANCE")) ?? "Server=localhost,1433;User Id=sa;Password=NStoreD0ck3r";
var msSqlDatabaseConnectionString  = msSqlServerConnectionString +";Database=NStore";

private string GetVariable(string key)
{
    var variable = Argument<string>(key, "___");
    Information("Variable "+key+" is " + variable);
    if(variable != "___")
        return variable;
    
    return EnvironmentVariable(key);
}

private string RemoveQuotes(string cstring)
{
    if(cstring == null)
        return null;
 
    if (cstring.StartsWith("\""))
        cstring = cstring.Substring(1);

    if (cstring.EndsWith("\""))
        cstring = cstring.Substring(0, cstring.Length - 1);

    return cstring;
}

private void RunTest(string testProject, IDictionary<string,string> env = null)
{
    var projectDir = "./src/"+ testProject + "/";
    var output = testOutput == "" ? "" :  "-xml " + testOutput + "/" + testProject + ".xml";

    var settings = new ProcessSettings
    {
//        Arguments = "xunit -parallel none",
        Arguments = "xunit " + output,
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

    if(testOutput != "")
    {
        CleanDirectory(testOutput);
    }

    EnsureDirectoryExists(testOutput);
});

Task("restore-packages")
    .IsDependentOn("Clean")
    .Does(() =>
{
    DotNetCoreRestore(solution);
   // NuGetRestore(solution);
});


Task("TestMsSql")
    .IsDependentOn("TestLibrary")
    .Does(()=>
{
    var dropdb = @"USE master
    IF EXISTS(select * from sys.databases where name='NStore')
    DROP DATABASE NStore
    ";

    var createdb = @"USE master 
    CREATE DATABASE NStore";

    var settings =  new SqlQuerySettings()
    {
        Provider = "MsSql",
        ConnectionString = msSqlServerConnectionString
    };

    Information("Connected to sql server instance " + msSqlServerConnectionString);

    ExecuteSqlQuery(dropdb, settings);
    ExecuteSqlQuery(createdb, settings);

    var env = new Dictionary<string, string>{
        { "NSTORE_MSSQL", msSqlDatabaseConnectionString},
    };
    
    RunTest("NStore.Persistence.MsSql.Tests",env);

    ExecuteSqlQuery(dropdb, settings);
});

Task("TestMongoDb")
    .IsDependentOn("TestLibrary")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "NSTORE_MONGODB", "mongodb://localhost/nstore-tests"},
    };

    RunTest("NStore.Persistence.Mongo.Tests",env);
});

Task("TestInMemory")
    .IsDependentOn("TestLibrary")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "xx", "val"},
    };

    RunTest("NStore.Persistence.Tests",env);
});


Task("TestSample")
    .IsDependentOn("TestLibrary")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "xx", "val"},
    };

    RunTest("NStore.Sample.Tests",env);
});

Task("TestLibrary")
    .IsDependentOn("restore-packages")
//    .IsDependentOn("Build")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "xx", "yy"},
    };

    RunTest("NStore.Tests",env);
});

Task("TestAll")
    .IsDependentOn("TestInMemory")
    .IsDependentOn("TestMongoDb")
    .IsDependentOn("TestMsSql")
    .IsDependentOn("TestSample")
    .Does(() =>
{
});

Task("ReleaseBuild")
    .IsDependentOn("TestAll")
    .Does(() =>
{
    Information("Building configuration "+configuration);
    var settings = new DotNetCoreBuildSettings
	{
		Configuration = configuration
	};

	DotNetCoreBuild(solution, settings);
});


//////////////////////////////////////////////////////////////////////
// TASK TARGETS
//////////////////////////////////////////////////////////////////////

Task("Default")
    .IsDependentOn("TestAll");

//////////////////////////////////////////////////////////////////////
// EXECUTION
//////////////////////////////////////////////////////////////////////

RunTarget(target);
