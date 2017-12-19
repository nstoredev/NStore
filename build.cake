#addin "Cake.SqlTools"

//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////
var target = Argument("target", "Default");
var configuration = Argument("configuration", "Release");

//////////////////////////////////////////////////////////////////////
// PREPARATION
//////////////////////////////////////////////////////////////////////
var msSqlServerConnectionString = RemoveQuotes(GetVariable("NSTORE_MSSQL_INSTANCE")) ?? "Server=localhost,1433;User Id=sa;Password=NStoreD0ck3r";
var msSqlDatabaseConnectionString  = msSqlServerConnectionString +";Database=NStore";
var testOutput = GetVariable("testoutput");
var version = Argument("nugetver","0.0.1-localbuild");

// Define Settings.
var artifactsRoot   = GetVariable("artifactsRoot") ?? "./artifacts";
var artifactsDir    = Directory(artifactsRoot);
var nugetDir        = Directory(artifactsRoot + "/nuget");
var solution        = "./src/NStore.sln";

private string GetVariable(string key)
{
    var variable = Argument<string>(key, "___");
    if(variable == "___")
    {
        variable = EnvironmentVariable(key);
    }
    Information("Variable "+key+" is <" + (variable == null ? "null" : variable) + ">");
    return variable;
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
    var to = GetVariable("testoutput");
    var output = to == null ? "" :  "-xml " + to + "/" + testProject + ".xml";

    var settings = new DotNetCoreToolSettings {
        WorkingDirectory = projectDir,
        EnvironmentVariables = env
    };

    DotNetCoreTool(projectDir +"/"+ testProject, "xunit", output, settings);
}


//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////

Task("Clean")
    .Does(() =>
{
    CleanDirectory(artifactsDir);

    if( testOutput != null )
    {
        CleanDirectory(testOutput);
        EnsureDirectoryExists(testOutput);
    }
});

Task("restore-packages")
    .IsDependentOn("Clean")
    .Does(() =>
{
    DotNetCoreRestore(solution);
   // NuGetRestore(solution);
});


Task("TestMsSql")
    .ContinueOnError()
    .IsDependentOn("TestLibrary")
    .IsDependentOn("TestMsSqlOnly")
    .Does(()=>
{
});

Task("TestMsSqlOnly")
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

Task("TestSqlite")
    .ContinueOnError()
    .IsDependentOn("TestLibrary")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "NSTORE_SQLITE", "TODO"},
    };

    RunTest("NStore.Persistence.Sqlite.Tests",env);
});

Task("TestMongoDb")
    .ContinueOnError()
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
    .ContinueOnError()
    .Does(() =>
{
    var env = new Dictionary<string, string>{};
    RunTest("NStore.Persistence.Tests",env);
});

Task("TestSample")
    .ContinueOnError()
    .IsDependentOn("TestLibrary")
    .Does(() =>
{
    var env = new Dictionary<string, string>{
        { "xx", "val"},
    };

    RunTest("NStore.Sample.Tests",env);
});

Task("TestLibrary")
    .ContinueOnError()
    .IsDependentOn("restore-packages")
    .Does(() =>
{
    var env = new Dictionary<string, string>{};

    RunTest("NStore.Core.Tests",env);
});


Task("TestDomain")
    .ContinueOnError()
    .IsDependentOn("TestLibrary")
    .Does(() =>
{
    var env = new Dictionary<string, string>{};
    RunTest("NStore.Domain.Tests",env);
});

Task("TestAll")
    .IsDependentOn("TestDomain")
    .IsDependentOn("TestInMemory")
    .IsDependentOn("TestMongoDb")
    .IsDependentOn("TestMsSql")
    .IsDependentOn("TestSqlite")
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

Task("pack")
    .Does(() =>
{
    Information("Packing");
    CleanDirectory(nugetDir);

    var settings = new DotNetCorePackSettings
    {
        ArgumentCustomization = args => args.Append("/p:Version=" + version),
        Configuration = "Release",
        OutputDirectory = nugetDir,
        NoBuild = true
    };

    DotNetCorePack("./src/NStore.Core/", settings);
    DotNetCorePack("./src/NStore.Domain/", settings);
    DotNetCorePack("./src/NStore.Tpl/", settings);
    DotNetCorePack("./src/NStore.Persistence.Mongo/", settings);
    DotNetCorePack("./src/NStore.Persistence.MsSql/", settings);
    DotNetCorePack("./src/NStore.Persistence.Sqlite/", settings);
});

//////////////////////////////////////////////////////////////////////
// TASK TARGETS
//////////////////////////////////////////////////////////////////////

Task("Default")
    .IsDependentOn("TestAll");

Task("Travis")
    .IsDependentOn("TestDomain")
    .IsDependentOn("TestInMemory")
    .IsDependentOn("TestMongoDb")
    .IsDependentOn("TestSqlite")
    .IsDependentOn("TestSample");


//////////////////////////////////////////////////////////////////////
// EXECUTION
//////////////////////////////////////////////////////////////////////

RunTarget(target);
