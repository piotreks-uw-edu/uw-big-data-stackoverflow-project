@echo off
setlocal enabledelayedexpansion

set BCP_EXPORT_SERVER=localhost
set BCP_EXPORT_DB=StackOverflow2013
set "CSV_SEPARATOR=,"

rem Define a list of user tables
set "TABLES=vBadges vComments vLinkTypes vPostLinks vPostTypes vUsers vVoteTypes vVotes"
@REM set "TABLES=vPosts"

rem Iterate through each table in the list
for %%T in (%TABLES%) do (
    set BCP_EXPORT_TABLE=%%T

    BCP "DECLARE @colnames VARCHAR(max);SELECT @colnames = COALESCE(@colnames + '%CSV_SEPARATOR%', '') + column_name from %BCP_EXPORT_DB%.INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='!BCP_EXPORT_TABLE!'; select @colnames;" queryout D:\csv2\HeadersOnly.csv -c -T -S%BCP_EXPORT_SERVER% -t"%CSV_SEPARATOR%"


    BCP %BCP_EXPORT_DB%.dbo.!BCP_EXPORT_TABLE! out D:\csv2\TableDataWithoutHeaders.csv -c -t"%CSV_SEPARATOR%" -T -S%BCP_EXPORT_SERVER%

    copy /b D:\csv2\HeadersOnly.csv+D:\csv2\TableDataWithoutHeaders.csv D:\csv2\!BCP_EXPORT_TABLE!.csv

    "C:\Program Files\7-Zip\7z.exe" a -tgzip D:\csv2\!BCP_EXPORT_TABLE!.csv.gz D:\csv2\!BCP_EXPORT_TABLE!.csv

    del D:\csv2\HeadersOnly.csv
    del D:\csv2\TableDataWithoutHeaders.csv
    del D:\csv2\!BCP_EXPORT_TABLE!.csv
)

