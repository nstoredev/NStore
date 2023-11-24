## 0.16.2

- Changed logic for MultiPartitionRead because it used index not global position and it is not correct.

## 0.16.1

- Updated Mongodb driver to 1.21
- Updated libraries for test (xunit, test runner)

## 0.16.0

- Externalized creation of Mongodb client to allow the creation of a single MongoClient

## 0.15.1

- Forced Mongodb drivers to use LINQ 2 provider due to excessive number of bugs for LINQ 3
- Added support for symbol server outside of azure devops with internal builds.

## 0.14-0.15

- Mostly driver updates.

## 0.13.0

- Added multi partition read.

## 0.12.1

- Fixed github action CI.

## 0.12.0

- Updated references
- Bumped full framework to 4.8

## 0.11.x

- Added ability to use a readonly connection on MongoDb persistence layer.

### Breaking Changes

- IPersistence does not support anymore the AppendAsync method, you should use a Stream class to have this behavior. This is part of removing the support of IPersistence for generation of Index value.

## 0.10.4

- Disabled parallelism on tests.

## 0.10.3

- Updated mongodb driver to latest version

## 0.10.2

- Updated mongodb driver to latest version

## 0.10.1

- Updated mongodb driver to latest version
- Fixed cake build.

## 0.9.0

- Minor fixes

## 0.8.2

- Fix wrong merge.

## 0.8.1

- Updated mongodb Driver 
- Updated to latest version of Newtonsoft.Json

## 0.8.0

- Minor modification to Sql Persistence.

## ...
