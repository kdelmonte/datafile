/*INPUT PARAMS
@dbName varchar(260),
@sessionTableId varchar(36),
@fileLocation varchar(260),
@columnsInFile varchar(max),
@fileHasColumnsNames bit,
@fieldDelimiter varchar(2),
@createdBy varchar(75),
@formatFilePath varchar(260)
*/

DECLARE 
@query varchar(max),
@targetTableLocation varchar(150),
@firstRow char(1),
@totalRecords int
	
IF(@fileHasColumnsNames = 1)
BEGIN
	SET @firstRow = '2'
END
ELSE
BEGIN
	SET @firstRow = '1'
END

SET @targetTableLocation = @dbName + '.dbo.[' + @sessionTableId + ']'
SET @query = 'CREATE TABLE ' + @targetTableLocation + '(' +
			  @columnsInFile
			  + ')'
PRINT @query + CHAR(13)			  
EXEC(@query)

IF(@fieldDelimiter = '') --FIXED WIDTH
BEGIN		
	SET @query = 'BULK INSERT ' + @targetTableLocation+ ' FROM ''' + @fileLocation + '''
	WITH 
	(
	FORMATFILE = ''' + @formatFilePath + ''',
	ROWTERMINATOR = ''\r\n'',
	FIRSTROW = ' + @firstRow + ',
	KEEPNULLS,
	TABLOCK
	)'
	PRINT @query + CHAR(13)	
	EXEC(@query)
	SET @totalRecords = @@ROWCOUNT
END
ELSE IF(@fieldDelimiter = '\t' or @fieldDelimiter = ',' or @fieldDelimiter = '|' )
BEGIN		
	SET @query = 'BULK INSERT ' + @targetTableLocation +
	' FROM ''' + @fileLocation + '''
	WITH
	(
	FIELDTERMINATOR = ''' + @fieldDelimiter + ''',
	ROWTERMINATOR = ''\n'',
	FIRSTROW = ' + @firstRow + ',
	TABLOCK,
	KEEPNULLS
	)'
	PRINT @query + CHAR(13)	
	EXEC(@query)
	SET @totalRecords = @@ROWCOUNT
END
ELSE
BEGIN
	RAISERROR('Unsupported Field Delimiter',17,1)	
END
			   
SET @query = '
			  ALTER TABLE ' + @targetTableLocation + '
			  ADD 
			  ___RecordId INT IDENTITY (1, 1) NOT NULL,
			  ___GroupId uniqueidentifier NULL
			  '
EXEC(@query)
