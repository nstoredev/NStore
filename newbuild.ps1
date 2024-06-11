param(
    [string] $nugetApiKey = "",
    [bool]   $nugetPublish = $false
)

Install-package BuildUtils -Confirm:$false -Scope CurrentUser -Force
Import-Module BuildUtils

$runningDirectory = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition

$nugetTempDir = "$runningDirectory/artifacts/NuGet"

if (Test-Path $nugetTempDir) 
{
    Write-host "Cleaning temporary nuget path $nugetTempDir"
    Remove-Item $nugetTempDir -Recurse -Force
}

dotnet tool restore
Assert-LastExecution -message "Unable to restore tooling." -haltExecution $true

$version = dotnet tool run dotnet-gitversion /config .config/gitversion.yml | Out-String | ConvertFrom-Json
Write-host "GitVersion output: $version"

Write-Verbose "Parsed value to be returned"
$assemblyVer = $version.AssemblySemVer 
$assemblyFileVersion = $version.AssemblySemFileVer
$nugetPackageVersion = $version.NuGetVersionV2
$assemblyInformationalVersion = $version.FullBuildMetaData

Write-host "assemblyInformationalVersion   = $assemblyInformationalVersion"
Write-host "assemblyVer                    = $assemblyVer"
Write-host "assemblyFileVersion            = $assemblyFileVersion"
Write-host "nugetPackageVersion            = $nugetPackageVersion"

# Now restore packages and build everything.
Write-Host "\n\n*******************RESTORING PACKAGES*******************"
dotnet restore "$runningDirectory/src/NStore.sln"
Assert-LastExecution -message "Error in restoring packages." -haltExecution $true

Write-Host "\n\n*******************TESTING SOLUTION*******************"
dotnet test "$runningDirectory/src/NStore.sln" `
    --collect:"XPlat Code Coverage" `
    --results-directory TestResults/ `
    --logger "trx;LogFileName=unittests.trx" `
    --no-restore `
    -- DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=opencover

Assert-LastExecution -message "Error in test running." -haltExecution $true

Write-Host "\n\n*******************BUILDING SOLUTION*******************"
dotnet build "$runningDirectory/src/NStore.sln" --configuration release
Assert-LastExecution -message "Error in building in release configuration" -haltExecution $true

Write-Host "\n\n*******************PUBLISHING SOLUTION*******************"
dotnet pack "$runningDirectory/src/NStore.Core/NStore.Core.csproj" --configuration release -o "$runningDirectory/artifacts/NuGet" /p:PackageVersion=$nugetPackageVersion /p:AssemblyVersion=$assemblyVer /p:FileVersion=$assemblyFileVer /p:InformationalVersion=$assemblyInformationalVersion
dotnet pack "$runningDirectory/src/NStore.Domain/NStore.Domain.csproj" --configuration release -o "$runningDirectory/artifacts/NuGet" /p:PackageVersion=$nugetPackageVersion /p:AssemblyVersion=$assemblyVer /p:FileVersion=$assemblyFileVer /p:InformationalVersion=$assemblyInformationalVersion
dotnet pack "$runningDirectory/src/NStore.Tpl/NStore.Tpl.csproj" --configuration release -o "$runningDirectory/artifacts/NuGet" /p:PackageVersion=$nugetPackageVersion /p:AssemblyVersion=$assemblyVer /p:FileVersion=$assemblyFileVer /p:InformationalVersion=$assemblyInformationalVersion
dotnet pack "$runningDirectory/src/NStore.BaseSqlPersistence/NStore.BaseSqlPersistence.csproj" --configuration release -o "$runningDirectory/artifacts/NuGet" /p:PackageVersion=$nugetPackageVersion /p:AssemblyVersion=$assemblyVer /p:FileVersion=$assemblyFileVer /p:InformationalVersion=$assemblyInformationalVersion
dotnet pack "$runningDirectory/src/NStore.Persistence.Mongo/NStore.Persistence.Mongo.csproj" --configuration release -o "$runningDirectory/artifacts/NuGet" /p:PackageVersion=$nugetPackageVersion /p:AssemblyVersion=$assemblyVer /p:FileVersion=$assemblyFileVer /p:InformationalVersion=$assemblyInformationalVersion
dotnet pack "$runningDirectory/src/NStore.Persistence.MsSql/NStore.Persistence.MsSql.csproj" --configuration release -o "$runningDirectory/artifacts/NuGet" /p:PackageVersion=$nugetPackageVersion /p:AssemblyVersion=$assemblyVer /p:FileVersion=$assemblyFileVer /p:InformationalVersion=$assemblyInformationalVersion
dotnet pack "$runningDirectory/src/NStore.Persistence.LiteDB/NStore.Persistence.LiteDB.csproj" --configuration release -o "$runningDirectory/artifacts/NuGet" /p:PackageVersion=$nugetPackageVersion /p:AssemblyVersion=$assemblyVer /p:FileVersion=$assemblyFileVer /p:InformationalVersion=$assemblyInformationalVersion
dotnet pack "$runningDirectory/src/NStore.Persistence.Sqlite/NStore.Persistence.Sqlite.csproj" --configuration release -o "$runningDirectory/artifacts/NuGet" /p:PackageVersion=$nugetPackageVersion /p:AssemblyVersion=$assemblyVer /p:FileVersion=$assemblyFileVer /p:InformationalVersion=$assemblyInformationalVersion

Assert-LastExecution -message "Error in creating nuget packages.." -haltExecution $true

if ($true -eq $nugetPublish) 
{
    Write-Host "\n\n*******************PUBLISHING NUGET PACKAGE*******************"
    dotnet nuget push .\artifacts\NuGet\** --source https://api.nuget.org/v3/index.json --api-key $nugetApiKey --skip-duplicate
    Assert-LastExecution -message "Error pushing nuget packages to nuget.org." -haltExecution $true
}
