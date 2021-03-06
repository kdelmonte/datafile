/*INPUT PARAMS
@dbName varchar(260),
@sessionTableId varchar(36),
@columns varchar(max),
*/

DECLARE 
@targetTableLocation varchar(150),
@query varchar(max)

SET @targetTableLocation = @dbName + '[' + @sessionTableId +  ']'	

--Select those records
SET @query = 
		'SELECT ' + @columns + '
			FROM ' + @targetTableLocation + '
			WHERE 1 = 2'
EXEC(@query)