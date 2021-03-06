/*INPUT PARAMS
@dbName varchar(260),
@sessionTableId varchar(36),
@updateClause varchar(max),
@whereClause varchar(max),
@groupId varchar(50),
@grouplessRecordsOnly bit
*/

DECLARE 
@targetTableLocation varchar(150),
@query varchar(max),
@groupIdAvailable bit,
@whereClauseAvailable bit

IF(@groupId IS NOT NULL AND LEN(RTRIM(LTRIM(@groupId))) > 0)
	SET @groupIdAvailable = 1
ELSE
	SET @groupIdAvailable = 0
	
IF(@whereClause IS NOT NULL AND LEN(RTRIM(LTRIM(@whereClause))) > 0)
	SET @whereClauseAvailable = 1
ELSE
	SET @whereClauseAvailable = 0

SET @targetTableLocation = @dbName + '[' + @sessionTableId +  ']'	

--Release group records
IF(@groupIdAvailable = 1)
BEGIN
	SET @query = 'UPDATE ' + @targetTableLocation + ' 
	SET ___GroupdId = NULL 
	WHERE ___GroupdId = ''' + @groupId + ''''
	PRINT(@query)
	EXEC(@query)
END

IF (@groupIdAvailable = 1 AND @grouplessRecordsOnly = 1)
BEGIN
	--Update Available Records with group id
	SET @query = 'UPDATE ' + @targetTableLocation + ' 
	SET ___GroupdId = ''' + @groupId + '''
	WHERE '
	IF(@whereClause IS NOT NULL AND LEN(RTRIM(LTRIM(@whereClause))) > 0)
		SET @query = @query + '(' + @whereClause + ') AND ___GroupdId IS NULL'
	ELSE
		SET @query = @query + ' ___GroupdId IS NULL'
	PRINT(@query)
	EXEC(@query)
	
	--Select those records
	SET @query = 
			'UPDATE ' + @updateClause + '
			 FROM ' + @targetTableLocation + '
			 WHERE ___GroupdId = ''' + @groupId + ''''
	EXEC(@query)
END
ELSE
BEGIN
	SET @query = 
			'UPDATE ' + @updateClause + ' 
			 FROM ' + @targetTableLocation
	IF(@whereClause IS NOT NULL AND LEN(RTRIM(LTRIM(@whereClause))) > 0)
		SET @query = @query + ' WHERE (' + @whereClause + ') '
	ELSE
	EXEC(@query)
END