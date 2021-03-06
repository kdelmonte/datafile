/*INPUT PARAMS
@dbName varchar(260),
@sessionTableId varchar(36),
@alterSchemaClause varchar(max)
*/

DECLARE 
@targetTableLocation varchar(150),
@query varchar(max)

SET @targetTableLocation = @dbName + '[' + @sessionTableId +  ']'	

SET @query = '
	ALTER TABLE ' + @targetTableLocation + '
	' + @alterSchemaClause + '
'
EXEC(@query)