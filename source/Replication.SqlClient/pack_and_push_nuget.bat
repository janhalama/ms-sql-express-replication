SET ver=1.4.1
SET projectName=Jh.Data.Sql.Replication.SqlClient

nuget pack %projectName%.csproj -Build -Symbols -IncludeReferencedProjects
nuget push %projectName%.%ver%.nupkg
nuget push %projectName%.%ver%.symbols.nupkg