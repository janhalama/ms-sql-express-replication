SET ver=1.8.0
SET projectName=Jh.Data.Sql.Replication.SqlClient
SET apiKey=%1
nuget pack %projectName%.csproj -Build -Symbols -IncludeReferencedProjects
nuget push %projectName%.%ver%.nupkg -Source https://api.nuget.org/v3/index.json -ApiKey %apiKey%
nuget push %projectName%.%ver%.symbols.nupkg -Source https://api.nuget.org/v3/index.json -ApiKey %apiKey%