SET ver=1.4.0.0
SET projectName=Jh.Data.Sql.Replication

nuget pack %projectName%.csproj -Build -Symbols -IncludeReferencedProjects
nuget push %projectName%.%ver%.nupkg
nuget push %projectName%.%ver%.symbols.nupkg