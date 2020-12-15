SET ver=1.7.0
SET projectName=Jh.Data.Sql.Replication.SqlClient
nuget pack %projectName%.csproj -Build -Symbols -IncludeReferencedProjects
nuget push %projectName%.%ver%.nupkg -Source https://api.nuget.org/v3/index.json -ApiKey oy2mtk3rryuhebwnud3qednf66wtn4eerjbrpjdrlg7ghy
nuget push %projectName%.%ver%.symbols.nupkg -Source https://api.nuget.org/v3/index.json -ApiKey oy2mtk3rryuhebwnud3qednf66wtn4eerjbrpjdrlg7ghy