@echo off
setlocal enabledelayedexpansion

set BCP_EXPORT_SERVER=localhost
set BCP_EXPORT_DB=StackOverflow2010

rem Define a list of user tables
set "TABLES=Badges Comments LinkTypes PostLinks PostTypes Posts Users VoteTypes Votes"

rem Iterate through each table in the list
for %%T in (%TABLES%) do (
    set BCP_EXPORT_TABLE=%%T

    BCP "DECLARE @colnames VARCHAR(max);SELECT @colnames = COALESCE(@colnames + '|||||', '') + column_name from %BCP_EXPORT_DB%.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='!BCP_EXPORT_TABLE!'; select @colnames;" queryout D:\csv\HeadersOnly.csv -c -T -S%BCP_EXPORT_SERVER% -t"|||||"


    BCP %BCP_EXPORT_DB%.dbo.!BCP_EXPORT_TABLE! out D:\csv\TableDataWithoutHeaders.csv -c -t"|||||" -T -S%BCP_EXPORT_SERVER%

    copy /b D:\csv\HeadersOnly.csv+D:\csv\TableDataWithoutHeaders.csv D:\csv\!BCP_EXPORT_TABLE!.csv

    del D:\csv\HeadersOnly.csv
    del D:\csv\TableDataWithoutHeaders.csv
)

endlocalaluse

endlocalusese