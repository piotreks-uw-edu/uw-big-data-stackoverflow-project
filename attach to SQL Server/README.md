USE [master]
GO
CREATE DATABASE [StackOverflow2013] ON 
( FILENAME = N'D:\Piotrek Documents\Szkolenia\2023_04_19 Big Data Technologies\uw-big-data-stackoverflow-project\db\StackOverflow2013_1.mdf' ),
( FILENAME = N'D:\Piotrek Documents\Szkolenia\2023_04_19 Big Data Technologies\uw-big-data-stackoverflow-project\db\StackOverflow2013_log.ldf' ),
( FILENAME = N'D:\Piotrek Documents\Szkolenia\2023_04_19 Big Data Technologies\uw-big-data-stackoverflow-project\db\StackOverflow2013_2.ndf' ),
( FILENAME = N'D:\Piotrek Documents\Szkolenia\2023_04_19 Big Data Technologies\uw-big-data-stackoverflow-project\db\StackOverflow2013_3.ndf' ),
( FILENAME = N'D:\Piotrek Documents\Szkolenia\2023_04_19 Big Data Technologies\uw-big-data-stackoverflow-project\db\StackOverflow2013_4.ndf' )
 FOR ATTACH
GO