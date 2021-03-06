/*INPUT PARAMS
@dbName varchar(50),
@sessionTableId varchar(36),
@columns varchar(max),
@whereClause varchar(max),
@groupByClause varchar(max),
@havingClause varchar(max),
@orderByClause varchar(max),
@groupId varchar(50),
@grouplessRecordsOnly bit,
@targetServer varchar(250),
@targetDB varchar(250),
@targetTable varchar(250)
*/

SET NOCOUNT ON
DECLARE
@targetTableLocation varchar(150),
@tempExportTable varchar(150),
@tempExportTableId varchar(36),
@query nvarchar(max),
@createTableQuery nvarchar(max),
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
	
SET @targetTableLocation = @dbName + '.dbo.[' + @sessionTableId +  ']'	
SET @tempExportTableId = NEWID()
SET @tempExportTable = @dbName + '.dbo.[' + @tempExportTableId + ']'
PRINT 'TEMP EXPORT TABLE: ' + @tempExportTable + CHAR(13)	

--Release group records
IF(@groupIdAvailable = 1)
BEGIN
	SET @query = 'UPDATE ' + @targetTableLocation + ' 
	SET ___GroupId = NULL 
	WHERE ___GroupId = ''' + @groupId + ''''
	EXEC(@query)
	
	IF(@grouplessRecordsOnly = 1)
	BEGIN
		--Update Available Records with group id
		SET @query = 'UPDATE ' + @targetTableLocation + ' 
		SET ___GroupId = ''' + @groupId + '''
		WHERE '
		IF(@whereClauseAvailable = 1)
			SET @query = @query + '(' + @whereClause + ') AND ___GroupId IS NULL'
		ELSE
			SET @query = @query + ' ___GroupId IS NULL'
		IF(@groupByClause IS NOT NULL AND LEN(RTRIM(LTRIM(@groupByClause))) > 0)
			SET @query = @query + ' GROUP BY ' + @groupByClause
		IF(@havingClause IS NOT NULL AND LEN(RTRIM(LTRIM(@havingClause))) > 0)
			SET @query = @query + ' HAVING ' + @havingClause
		IF(@orderByClause IS NOT NULL AND LEN(RTRIM(LTRIM(@orderByClause))) > 0)
			SET @query = @query + ' ORDER BY ' + @orderByClause
		EXEC(@query)
	END
END



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


IF(@targetServer IS NULL) SET @targetServer = @@servername
IF(SUBSTRING(LTRIM(RTRIM(@targetServer)),1,1) <> '[') SET @targetServer = QUOTENAME(@targetServer)
IF(SUBSTRING(LTRIM(RTRIM(@targetDB)),1,1) <> '[') SET @targetDB = QUOTENAME(@targetDB)
IF(SUBSTRING(LTRIM(RTRIM(@targetTable)),1,1) <> '[') SET @targetTable = QUOTENAME(@targetTable)

--SELECT INTO does not work for linked servers so this is just a work around to be able to create a table
SET @query = 'EXEC(''IF OBJECT_ID(''''' + @targetDB + '.[dbo].' + @targetTable + ''''') IS NOT NULL 
					BEGIN
						DROP TABLE ' + @targetDB + '.[dbo].' + @targetTable + '
					END	
			 '
SELECT @createTableQuery = ' CREATE TABLE ' + @targetDB + '.[dbo].' + @targetTable + '  (' + SUBSTRING(RTRIM(LTRIM(o.list)),0, LEN(RTRIM(LTRIM(o.list))) - 1) + ')' +
CASE 
	WHEN tc.Constraint_Name IS NULL THEN '' 
	ELSE 'ALTER TABLE ' + so.Name + ' ADD CONSTRAINT ' + tc.Constraint_Name  + ' PRIMARY KEY ' + ' (' + LEFT(j.List, Len(j.List)-1) + ')' 
END
from    tempdb.sys.sysobjects so
cross apply
	(SELECT 
		' [' + column_name + '] ' + 
		data_type + case data_type
				when 'sql_variant' then ''
				when 'text' then ''
				when 'decimal' then '(' + cast(numeric_precision as varchar) + ', ' + cast(numeric_scale as varchar) + ')'
				else coalesce('('+case when character_maximum_length = - 1 then 'MAX' else cast(character_maximum_length as varchar) end +')','') end + ' ' +
		case when exists ( 
		select id from tempdb.sys.syscolumns
		where object_name(id)=so.name
		and name=column_name
		and columnproperty(id,name,'IsIdentity') = 1 
		) then
		'IDENTITY(' + 
		cast(ident_seed(so.name) as varchar) + ',' + 
		cast(ident_incr(so.name) as varchar) + ')'
		else ''
		end + ' ' +
		 (case when IS_NULLABLE = 'No' then 'NOT ' else '' end ) + 'NULL ' + 
		  case when tempdb.information_schema.columns.COLUMN_DEFAULT IS NOT NULL THEN 'DEFAULT ' + tempdb.information_schema.columns.COLUMN_DEFAULT ELSE '' END + ', '
	 from tempdb.information_schema.columns where table_name = so.name
	 order by ordinal_position
	FOR XML PATH('')) o (list)
left join
	tempdb.information_schema.table_constraints tc
on  tc.Table_name = so.Name
AND tc.Constraint_Type  = 'PRIMARY KEY'
cross apply
	(select '[' + Column_Name + '], '
	 FROM       tempdb.information_schema.key_column_usage kcu
	 WHERE      kcu.Constraint_Name     = tc.Constraint_Name
	 ORDER BY
		ORDINAL_POSITION
	 FOR XML PATH('')) j (list)
where   xtype = 'U'
AND name = @tempExportTableId

SET @createTableQuery = REPLACE(@createTableQuery,'''','''''')
SET @query = @query + @createTableQuery + ''') AT ' + @targetServer

EXEC(@query)

SET @query = 'INSERT INTO ' + @targetServer + '.' + @targetDB + '.[dbo].' + @targetTable + '  
			  SELECT * 
			  FROM ' + @tempExportTable
EXEC(@query)
	
SET @query = 'DROP TABLE ' + @tempExportTable
EXEC(@query)
