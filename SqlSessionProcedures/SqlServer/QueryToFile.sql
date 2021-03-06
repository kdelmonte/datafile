/*INPUT PARAMS
@dbName varchar(260),
@sessionTableId varchar(36),
@columns varchar(max),
@whereClause varchar(max),
@groupByClause varchar(max),
@havingClause varchar(max),
@orderByClause varchar(max),
@targetFilePath varchar(260),
@groupId varchar(50),
@grouplessRecordsOnly bit,
@numberOfRecordsCopied int, --out
@fieldDelimiter varchar(10)
*/

SET NOCOUNT ON
DECLARE 
@targetTableLocation varchar(150),
@tempExportTable varchar(150),
@tempExportTableId varchar(36),
@cmd nvarchar(3000),
@query nvarchar(max),
@headers varchar(max),
@bcpError varchar(max),
@bcpError2 varchar(max),
@bcpConcatenatedError nvarchar(512),
@groupIdAvailable bit,
@whereClauseAvailable bit,
@textQualifierUpdateStatement varchar(max),
@columnName varchar(500),
@columnMaxLength int

DECLARE @ExportTableColumns TABLE(
  ID int IDENTITY(1,1) PRIMARY KEY,
  ColumnName varchar(150),
  MaxLength int
)
	
IF(@groupId IS NOT NULL AND LEN(RTRIM(LTRIM(@groupId))) > 0)
	SET @groupIdAvailable = 1
ELSE
	SET @groupIdAvailable = 0
	
IF(@whereClause IS NOT NULL AND LEN(RTRIM(LTRIM(@whereClause))) > 0)
	SET @whereClauseAvailable = 1
ELSE
	SET @whereClauseAvailable = 0
	
--Release group records
IF(@groupIdAvailable = 1)
BEGIN
	SET @query = 'UPDATE ' + @targetTableLocation + ' 
	SET ___GroupdId = NULL 
	WHERE ___GroupdId = ''' + @groupId + ''''
	--PRINT(@query)
	EXEC(@query)
	
	IF(@grouplessRecordsOnly = 1)
	BEGIN
		--Update Available Records with group id
		SET @query = 'UPDATE ' + @targetTableLocation + ' 
		SET ___GroupdId = ''' + @groupId + '''
		WHERE '
		IF(@whereClauseAvailable = 1)
			SET @query = @query + '(' + @whereClause + ') AND ___GroupdId IS NULL'
		ELSE
			SET @query = @query + ' ___GroupdId IS NULL'
		IF(@groupByClause IS NOT NULL AND LEN(RTRIM(LTRIM(@groupByClause))) > 0)
			SET @query = @query + ' GROUP BY ' + @groupByClause
		IF(@havingClause IS NOT NULL AND LEN(RTRIM(LTRIM(@havingClause))) > 0)
			SET @query = @query + ' HAVING ' + @havingClause
		IF(@orderByClause IS NOT NULL AND LEN(RTRIM(LTRIM(@orderByClause))) > 0)
			SET @query = @query + ' ORDER BY ' + @orderByClause
		EXEC(@query)
	END
END

DECLARE @bcpOutput TABLE (id int IDENTITY, command nvarchar(256))

SET @headers = '''' + @columns + ''''
SET @headers = REPLACE(@headers,',',''',''')
SET @headers = REPLACE(REPLACE(@headers,'[',''),']','')

SET @targetTableLocation = @dbName + '[' + @sessionTableId +  ']'	
SET @tempExportTableId = NEWID()
SET @tempExportTable = @dbName + '[' + @tempExportTableId + ']'
PRINT 'TEMP EXPORT TABLE: ' + @tempExportTable + CHAR(13)	

SET @query = 'SELECT ' + @columns + ' 
			  INTO ' + @tempExportTable + '
			  FROM ' + @targetTableLocation 
IF(@whereClauseAvailable = 1)
	SET @query = @query + ' WHERE (' + @whereClause + ') '
IF (@groupIdAvailable = 1 AND @grouplessRecordsOnly = 1)
BEGIN
	IF(@whereClauseAvailable = 0)SET @query = @query + ' WHERE '
	ELSE SET @query = @query + ' AND '
	SET @query = @query + ' ___GroupId = ''' + @groupId + ''''
END
IF(@groupByClause IS NOT NULL AND LEN(RTRIM(LTRIM(@groupByClause))) > 0)
	SET @query = @query + ' GROUP BY ' + @groupByClause
IF(@havingClause IS NOT NULL AND LEN(RTRIM(LTRIM(@havingClause))) > 0)
	SET @query = @query + ' HAVING ' + @havingClause
IF(@orderByClause IS NOT NULL AND LEN(RTRIM(LTRIM(@orderByClause))) > 0)
	SET @query = @query + ' ORDER BY ' + @orderByClause  
EXEC(@query)

IF(LEN(@fieldDelimiter) > 0)
BEGIN
	INSERT INTO @ExportTableColumns (ColumnName,MaxLength)
	SELECT COLUMN_NAME,CHARACTER_MAXIMUM_LENGTH
	FROM TEMPDB.INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_NAME = @tempExportTable

	DECLARE db_cursor CURSOR FOR  
	SELECT ColumnName,MaxLength
	FROM @ExportTableColumns

	OPEN db_cursor  
	FETCH NEXT FROM db_cursor INTO @columnName,@columnMaxLength

	WHILE @@FETCH_STATUS = 0  
	BEGIN	   
		   SET @query = '
						
						DECLARE @valuesThatNeedTextQualifier bigint
						SELECT @valuesThatNeedTextQualifier = COUNT(*)
						FROM ' + @tempExportTable + '
						WHERE [' + @columnName + ']  LIKE ''%' + @fieldDelimiter + '%'' 
						
						IF(@valuesThatNeedTextQualifier > 0)
						BEGIN
							ALTER TABLE ' + @tempExportTable + '
							ALTER COLUMN [' + @columnName + '] CHAR(' + CONVERT(VARCHAR(20),(@columnMaxLength + 2)) + ')
							
							UPDATE ' + @tempExportTable + '
							SET [' + @columnName + '] = QUOTENAME(SUBSTRING([' + @columnName + '],0,' + CONVERT(VARCHAR(20),(@columnMaxLength + 1)) + '),CHAR(34))
							WHERE [' + @columnName + ']  LIKE ''%' + @fieldDelimiter + '%'' 
						END							
						
		   '
		   EXEC(@query)
		   FETCH NEXT FROM db_cursor INTO @columnName,@columnMaxLength 
	END  

	CLOSE db_cursor  
	DEALLOCATE db_cursor
END
 
	
SET @cmd = 'bcp " select * from ' + @tempExportTable	
SET @cmd = 
		@cmd + '" 
		 queryout "' + @targetFilePath + '" 
		-c  
		-t"' + @fieldDelimiter + '" 
		-T 
		-S' + @@servername	
SET @cmd = REPLACE(REPLACE(REPLACE(@cmd, CHAR(10), ''), CHAR(13), ''), CHAR(9), '')
PRINT @cmd
INSERT INTO @bcpOutput
	exec xp_cmdShell @cmd
	
SET @query = 'DROP TABLE ' + @tempExportTable
PRINT(@query)
EXEC(@query)

SELECT @bcpError = command FROM @bcpOutput WHERE Id = 1
IF(@bcpError IS NOT NULL)
BEGIN
	
	SELECT @bcpError2 = command FROM @bcpOutput WHERE Id = 1
	SET @bcpConcatenatedError = @bcpError + ' : ' + @bcpError2
	RAISERROR(@bcpConcatenatedError ,17,1)
END
SELECT @numberOfRecordsCopied = CONVERT(INT,REPLACE(command,' rows copied.','')) 
FROM @bcpOutput
WHERE Id = (SELECT MAX(Id) - 3 FROM @bcpOutput)

SELECT * FROM @bcpOutput
