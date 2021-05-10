Param(
[string] $buildPrefix
)
Install-package BuildUtils -Confirm:$false -Scope CurrentUser -Force
Import-Module BuildUtils

$runningDirectory = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition

$version = Invoke-Gitversion
$assemblyVer = $version.assemblyVersion 
$assemblyFileVersion = $version.assemblyFileVersion
$nugetPackageVersion = $version.nugetVersion
$assemblyInformationalVersion = $version.assemblyInformationalVersion

$buildId = $env:BUILD_BUILDID
Write-Host "Build id variable is $buildId"
if (![System.String]::IsNullOrEmpty($buildId)) 
{
    Write-Host "Running in an Azure Devops Build"

    Write-Host "##vso[build.updatebuildnumber]$buildPrefix - $($version.fullSemver)"
    Write-Host "##vso[task.setvariable variable=NugetVersion;]$nugetPackageVersion"
}

Update-SourceVersion -SrcPath "$runningDirectory/src" `
    -assemblyVersion $version.assemblyVersion `
    -fileAssemblyVersion $version.assemblyFileVersion `
    -assemblyInformationalVersion $version.assemblyInformationalVersion